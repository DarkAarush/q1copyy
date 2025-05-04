import logging
import asyncio
from telegram import Update
from telegram.ext import ContextTypes
from chat_data_handler import load_chat_data, save_chat_data
from leaderboard_handler import add_score
import random
import aiofiles
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from telegram.error import BadRequest, TimedOut, NetworkError, RetryAfter
import redis.asyncio as redis
import msgpack  # For compact serialization
import zlib  # For compression

# Logger setup
logger = logging.getLogger(__name__)

# Async MongoDB connection
MONGO_URI = "mongodb+srv://2004:2005@cluster0.6vdid.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = AsyncIOMotorClient(MONGO_URI)
db = client["telegram_bot"]
quizzes_sent_collection = db["quizzes_sent"]
used_quizzes_collection = db["used_quizzes"]
message_status_collection = db["message_status"]

# Async Redis connection
try:
    redis_client = redis.StrictRedis(host="127.0.0.1", port=6379, db=0, decode_responses=False)
except Exception as e:
    logger.error(f"Failed to connect to Redis: {e}")
    redis_client = None  # Set to None if Redis is unavailable

# Helper functions for compression and serialization
def compress_data(data):
    """
    Compress data using zlib and serialize with MessagePack.
    """
    return zlib.compress(msgpack.packb(data))

def decompress_data(data):
    """
    Decompress data using zlib and deserialize with MessagePack.
    """
    return msgpack.unpackb(zlib.decompress(data), raw=False)

def retry_on_failure(func):
    """
    Decorator to retry a function on transient errors.
    """
    async def wrapper(*args, **kwargs):
        retries = 3
        while retries > 0:
            try:
                return await func(*args, **kwargs)
            except (TimedOut, NetworkError, RetryAfter) as e:
                logger.warning(f"Retryable error occurred: {e}. Retrying...")
                retries -= 1
            except Exception as e:
                logger.error(f"Unrecoverable error occurred: {e}")
                break
        logger.error(f"Function {func.__name__} failed after retries.")
    return wrapper

@retry_on_failure
async def load_quizzes(category):
    """
    Load quizzes from file or cache using asynchronous file I/O and caching.
    """
    if redis_client:
        try:
            cache_key = f"quizzes:{category}"
            cached_data = await redis_client.get(cache_key)
            if cached_data:
                return decompress_data(cached_data)
        except Exception as e:
            logger.error(f"Redis error while loading quizzes: {e}")

    # Fallback to file read if not cached
    file_path = f"quizzes/{category}.json"
    try:
        async with aiofiles.open(file_path, mode="r") as f:
            quizzes = await f.read()
            quizzes = json.loads(quizzes)
            if redis_client:
                try:
                    await redis_client.set(cache_key, compress_data(quizzes), ex=3600)  # Cache for 1 hour
                except Exception as e:
                    logger.error(f"Redis error while caching quizzes: {e}")
            return quizzes
    except FileNotFoundError:
        logger.error(f"Quiz file for category '{category}' not found.")
        return []

async def get_daily_quiz_limit(chat_type):
    """
    Set daily quiz limit based on chat type.
    """
    if chat_type == 'private':
        return 5  # Daily limit for private chats
    else:
        return 10  # Daily limit for groups/supergroups

@retry_on_failure
async def send_quiz_logic(context: ContextTypes.DEFAULT_TYPE, chat_id: str):
    """
    Core logic for sending a quiz to the specified chat.
    """
    chat_data = await load_chat_data(chat_id)
    category = chat_data.get("category", "default")
    questions = await load_quizzes(category)

    # Get the chat type and daily quiz limit
    chat_type = (await context.bot.get_chat(chat_id)).type  # Get chat type (private, group, or supergroup)
    logger.info(f"Chat ID: {chat_id} | Chat Type: {chat_type}")

    today = datetime.now().date().isoformat()  # Convert date to string
    quizzes_sent = await quizzes_sent_collection.find_one({"chat_id": chat_id, "date": today})
    message_status = await message_status_collection.find_one({"chat_id": chat_id, "date": today})

    daily_limit = await get_daily_quiz_limit(chat_type)  # Pass chat_type to get_daily_quiz_limit
    logger.info(f"Daily quiz limit for chat type '{chat_type}': {daily_limit}")
    
    if quizzes_sent is None:
        await quizzes_sent_collection.insert_one({"chat_id": chat_id, "date": today, "count": 0})  # Initialize count with 0
        quizzes_sent = {"count": 0}  # Ensure quizzes_sent has a default structure

    # Check if the daily limit is reached
    if quizzes_sent["count"] >= daily_limit:
        # Send confirmation message immediately when the limit is first reached
        if message_status is None or not message_status.get("limit_reached", False):
            await context.bot.send_message(chat_id=chat_id, text="Your daily limit is reached. You will get quizzes tomorrow.")
            if message_status is None:
                await message_status_collection.insert_one({"chat_id": chat_id, "date": today, "limit_reached": True})
            else:
                await message_status_collection.update_one({"chat_id": chat_id, "date": today}, {"$set": {"limit_reached": True}})
        return  # Stop further processing

    if not questions:
        await context.bot.send_message(chat_id=chat_id, text="No questions available for this category.")
        return

    # Fetch used questions from MongoDB
    used_questions = await used_quizzes_collection.find_one({"chat_id": chat_id})
    used_questions = used_questions["used_questions"] if used_questions else []

    # Filter available questions
    available_questions = [q for q in questions if q not in used_questions]
    if not available_questions:
        # Reset used questions if no new questions are available
        await used_quizzes_collection.update_one({"chat_id": chat_id}, {"$set": {"used_questions": []}})
        used_questions = []
        available_questions = questions
        await context.bot.send_message(chat_id=chat_id, text="All quizzes have been used. Restarting with all available quizzes.")

    # Select a question
    question = random.choice(available_questions)
    await used_quizzes_collection.update_one(
        {"chat_id": chat_id}, {"$push": {"used_questions": question}}, upsert=True
    )

    try:
        # Send the quiz as a poll
        message = await context.bot.send_poll(
            chat_id=chat_id,
            question=question["question"],
            options=question["options"],
            type="quiz",
            correct_option_id=question["correct_option_id"],
            is_anonymous=False,
        )
        # Log the quiz sent
        await quizzes_sent_collection.update_one(
            {"chat_id": chat_id, "date": today},
            {"$inc": {"count": 1}},
        )
        # Store poll data in bot's memory
        context.bot_data[message.poll.id] = {
            "chat_id": chat_id,
            "correct_option_id": question["correct_option_id"],
        }
    except BadRequest as e:
        logger.error(f"Failed to send quiz to chat {chat_id}: {e}. Sending next quiz...")
        # Retry sending the next quiz
        await send_quiz_logic(context, chat_id)

async def send_quiz(context: ContextTypes.DEFAULT_TYPE):
    """
    Asynchronous wrapper to send a quiz to the chat.
    """
    chat_id = context.job.data["chat_id"]  # Updated to use data instead of context
    await send_quiz_logic(context, chat_id)

async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle user answers to polls and update leaderboard scores.
    """
    poll_answer = update.poll_answer
    user_id = str(poll_answer.user.id)
    selected_option = poll_answer.option_ids[0] if poll_answer.option_ids else None

    poll_id = poll_answer.poll_id
    poll_data = context.bot_data.get(poll_id)

    if not poll_data:
        logger.warning(f"No poll data found for poll_id {poll_id}")
        return

    correct_option_id = poll_data["correct_option_id"]

    # Update the score if the user's answer is correct
    if selected_option == correct_option_id:
        await add_score(user_id, 1)

async def send_quiz_immediately(context: ContextTypes.DEFAULT_TYPE, chat_id: str):
    """
    Send a quiz immediately to the specified chat.
    """
    await send_quiz_logic(context, chat_id)
