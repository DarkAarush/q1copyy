import os
from motor.motor_asyncio import AsyncIOMotorClient  # Async MongoDB client
import redis.asyncio as redis  # Async Redis client
import json
import logging

# Logger setup
logger = logging.getLogger(__name__)

# Async MongoDB connection
MONGO_URI = "mongodb+srv://2004:2005@cluster0.6vdid.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = AsyncIOMotorClient(MONGO_URI)
db = client["telegram_bot"]
chat_data_collection = db["chat_data"]
served_chats_collection = db["served_chats"]
served_users_collection = db["served_users"]

# Async Redis connection with retry and fallback
try:
    redis_client = redis.StrictRedis(host="127.0.0.1", port=6379, db=0, decode_responses=True)
    logger.info("Connected to Redis successfully.")
except Exception as e:
    logger.error(f"Failed to connect to Redis

async def load_chat_data(chat_id=None):
    """
    Load chat data from MongoDB or cache.
    """
    if chat_id:
        cached_data = await redis_client.get(f"chat_data:{chat_id}")
        if cached_data:
            return json.loads(cached_data)

        chat_data = await chat_data_collection.find_one({"chat_id": chat_id}, {"data": 1})  # Use projection
        if chat_data:
            await redis_client.set(f"chat_data:{chat_id}", json.dumps(chat_data["data"]), ex=3600)  # Cache for 1 hour
            return chat_data["data"]
        return {}
    else:
        cursor = chat_data_collection.find({}, {"chat_id": 1, "data": 1})  # Projection for required fields
        return {chat["chat_id"]: chat["data"] async for chat in cursor}

async def save_chat_data(chat_id, data):
    """
    Save chat data to MongoDB and invalidate cache.
    """
    await chat_data_collection.update_one(
        {"chat_id": chat_id},
        {"$set": {"data": data}},
        upsert=True
    )
    await redis_client.delete(f"chat_data:{chat_id}")  # Invalidate cache

async def get_served_chats():
    """
    Retrieve all served chats.
    """
    cursor = served_chats_collection.find({}, {"chat_id": 1})
    return [chat["chat_id"] async for chat in cursor]

async def get_served_users():
    """
    Retrieve all served users.
    """
    cursor = served_users_collection.find({}, {"user_id": 1})
    return [user["user_id"] async for user in cursor]

async def add_served_chat(chat_id):
    """
    Add a chat to the served chats collection.
    """
    await served_chats_collection.update_one(
        {"chat_id": chat_id},
        {"$set": {"chat_id": chat_id}},
        upsert=True
    )

async def add_served_user(user_id):
    """
    Add a user to the served users collection.
    """
    await served_users_collection.update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id}},
        upsert=True
    )

# Get all active quizzes
def get_active_quizzes():
    return chat_data_collection.find({"data.active": True})
