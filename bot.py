import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMember
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes, PollAnswerHandler
)
from chat_data_handler import (
    load_chat_data, save_chat_data, add_served_chat, add_served_user, get_active_quizzes
)
from quiz_handler import send_quiz, send_quiz_immediately, handle_poll_answer
from leaderboard_handler import get_user_score, get_top_scores
from admin_handler import broadcast
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio

# Enable logging
from bot_logging import logger

# Bot token and admin details
TOKEN = "7183336129:AAGC7Cj0fXjMQzROUXMZHnb0pyXQQqneMic"
ADMIN_ID = 5050578106
LOG_GROUP_ID = -1001902619247

# MongoDB connection
MONGO_URI = "mongodb+srv://2004:2005@cluster0.6vdid.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = AsyncIOMotorClient(MONGO_URI)
db = client["telegram_bot"]

# Function to test MongoDB connection
async def test_mongo_connection():
    try:
        await client.admin.command("ping")
        logger.info("MongoDB connection successful.")
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        raise

# Log user or group when they start the bot
async def log_user_or_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    log_message = ""

    if chat.type in ['group', 'supergroup']:
        log_message = (
            f"Group started the bot: {chat.title}\nID: {chat.id}\n"
            f"Group link: https://t.me/{chat.username if chat.username else 'N/A'}"
        )
    else:
        log_message = (
            f"User started the bot: {user.first_name} {user.last_name or ''}\n"
            f"Username: @{user.username or 'N/A'}, ID: {user.id}\n"
            f"User profile: https://t.me/{user.username if user.username else 'N/A'}"
        )

    logger.info(f"Logging message: {log_message}")
    await context.bot.send_message(chat_id=LOG_GROUP_ID, text=log_message)

# /start command to display main menu
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    user_id = str(update.effective_user.id)

    try:
        # Log the user or group
        await log_user_or_group(update, context)

        # Register the chat and user for broadcasting
        await add_served_chat(chat_id)
        await add_served_user(user_id)

        # Inline buttons for the main menu
        keyboard = [
            [InlineKeyboardButton("Add in Your Group +", url=f"https://t.me/PYQ_Quizbot?startgroup=true")],
            [InlineKeyboardButton("Start PYQ Quizzes", callback_data='start_quiz')],
            [
                InlineKeyboardButton("📊 Leaderboard", callback_data='show_leaderboard'),
                InlineKeyboardButton("📈 My Score", callback_data='show_stats')
            ],
            [InlineKeyboardButton("Commands", callback_data='show_commands')],
            [InlineKeyboardButton("Download all Edition Book", url=f"https://t.me/+ZSZUt_eBmmhiMDM1")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # Send welcome message with main menu buttons
        await update.message.reply_text(
            "*Welcome to Pinnacle 7th Edition Quiz Bot!*\n\n"
            "This bot allows you to take PYQ quizzes for the following exams:\n"
            "- SSC\n"
            "- RRB\n\n"
            "Choose an option below to get started:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error(f"Error in start_command: {e}")
        await update.message.reply_text("An error occurred while starting the bot. Please try again later.")

# Button callback handler
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()  # Acknowledge the button click
    chat_id = str(query.message.chat.id)
    chat_data = await load_chat_data(chat_id)

    try:
        if query.data == 'start_quiz':
            # Show language selection
            keyboard = [
                [
                    InlineKeyboardButton("Hindi", callback_data='language_hindi'),
                    InlineKeyboardButton("English", callback_data='language_english')
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(
                text="*Please select your language:*",
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )

        elif query.data.startswith('language_'):
            # Handle language selection
            language = query.data.split('_')[1]
            chat_data['language'] = language
            await save_chat_data(chat_id, chat_data)

            # Show category selection
            keyboard = [
                [
                    InlineKeyboardButton("SSC", callback_data=f'category_SSC_{language}'),
                    InlineKeyboardButton("RRB", callback_data=f'category_RRB_{language}')
                ],
                [InlineKeyboardButton("Back", callback_data='start_quiz')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(
                text=f"*Language selected: {language.upper()}*\nPlease select your category:",
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )

        elif query.data.startswith('category_'):
            # Handle category selection
            category = query.data.split('_')[1]
            chat_data['category'] = category
            await save_chat_data(chat_id, chat_data)

            # Show interval selection
            keyboard = [
                [
                    InlineKeyboardButton("30 sec", callback_data='interval_30'),
                    InlineKeyboardButton("1 min", callback_data='interval_60'),
                    InlineKeyboardButton("5 min", callback_data='interval_300')
                ],
                [InlineKeyboardButton("Back", callback_data='start_quiz')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(
                text=f"*Category selected: {category.upper()}*\nPlease select the interval:",
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )

        elif query.data.startswith('interval_'):
            # Handle interval selection
            interval = int(query.data.split('_')[1])
            chat_data["interval"] = interval
            await save_chat_data(chat_id, chat_data)

            # Start the quiz
            await query.edit_message_text(
                f"Quiz interval set to {interval} seconds. Starting quiz...",
                parse_mode="Markdown"
            )
            await start_quiz(update, context)

        elif query.data == 'show_leaderboard':
            # Show leaderboard
            await show_leaderboard(update, context)

        elif query.data == 'show_stats':
            # Show user's score
            user_id = str(update.effective_user.id)
            score = await get_user_score(user_id)
            await query.edit_message_text(f"Your current score: {score} points.")

        elif query.data == 'show_commands':
            # Show available commands
            commands = (
                "/start - Start the bot\n"
                "/setinterval <seconds> - Set quiz interval\n"
                "/stopquiz - Stop the current quiz\n"
                "/pause - Pause the quiz\n"
                "/resume - Resume the quiz\n"
                "/leaderboard - Show the leaderboard\n"
                "/stats - Show your stats"
            )
            await query.edit_message_text(f"*Available Commands:*\n\n{commands}", parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Error handling button callback: {e}")
        await query.edit_message_text("An error occurred while processing your request. Please try again later.")


def is_user_admin(update: Update, user_id: int):
    chat_member = update.effective_chat.get_member(user_id)
    return chat_member.status in [ChatMember.ADMINISTRATOR, ChatMember.CREATOR]

# def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     chat_id = str(update.effective_chat.id)
#     query = update.callback_query
#     query.answer()
#     chat_id = str(query.message.chat.id)
#     chat_data = load_chat_data(chat_id)

#     if query.data == 'start_quiz':
#         # Inline buttons for language selection
#         keyboard = [
#             [
#                 InlineKeyboardButton("Hindi", callback_data='language_hindi'),
#                 InlineKeyboardButton("English", callback_data='language_english')
#             ]
#         ]
#         reply_markup = InlineKeyboardMarkup(keyboard)
#         query.edit_message_text(text="*Please select your language: [Hindi, English]*", reply_markup=reply_markup, parse_mode="Markdown")

#     elif query.data.startswith('language_'):
#         language = query.data.split('_')[1]
#         chat_data['language'] = language
#         save_chat_data(chat_id, chat_data)

#         # Inline buttons for category selection based on the chosen language
#         if language == 'hindi':
#             keyboard = [
#                 [
#                     InlineKeyboardButton("SSC", callback_data='category_SSCHi'),
#                     InlineKeyboardButton("RRB", callback_data='category_RRBHi')
#                 ],
#                 [InlineKeyboardButton("Back", callback_data='back_to_languages')]
#             ]
#         elif language == 'english':
#             keyboard = [
#                 [
#                     InlineKeyboardButton("SSC", callback_data='category_SSCEn'),
#                     InlineKeyboardButton("RRB", callback_data='category_RRBEn')
#                 ],
#                 [InlineKeyboardButton("Back", callback_data='back_to_languages')]
#             ]
        
#         reply_markup = InlineKeyboardMarkup(keyboard)
#         query.edit_message_text(text=f"*Language selected: {language.upper()}\nPlease select your category: [SSC, RRB]*",
#                                 reply_markup=reply_markup, parse_mode="Markdown")

#     elif query.data.startswith('category_'):
#         category = query.data.split('_')[1]
#         chat_data['category'] = category
#         save_chat_data(chat_id, chat_data)

#         # Directly start the quiz with interval selection
#         keyboard = [
#             [
#                 InlineKeyboardButton("30 sec", callback_data='interval_30'),
#                 InlineKeyboardButton("1 min", callback_data='interval_60'),
#                 InlineKeyboardButton("5 min", callback_data='interval_300')
#             ],
#             [
#                 InlineKeyboardButton("10 min", callback_data='interval_600'),
#                 InlineKeyboardButton("30 min", callback_data='interval_1800'),
#                 InlineKeyboardButton("60 min", callback_data='interval_3600')
#             ]
#         ]
#         reply_markup = InlineKeyboardMarkup(keyboard)
#         query.edit_message_text(text=f"Category selected: {category.upper()}\n*Please select the interval for quizzes: using this command /setinterval *\nSet the interval for quizzes - [Ex. /setinterval 20] for set Custom Interval",
#                                 reply_markup=reply_markup, parse_mode="Markdown")

#     elif query.data == 'back_to_languages':
#         # Inline buttons for language selection
#         keyboard = [
#             [
#                 InlineKeyboardButton("Hindi", callback_data='language_hindi'),
#                 InlineKeyboardButton("English", callback_data='language_english')
#             ]
#         ]
#         reply_markup = InlineKeyboardMarkup(keyboard)
#         query.edit_message_text(text="Please select your language:", reply_markup=reply_markup)

#     elif query.data == 'back_to_categories':
#         language = chat_data.get('language', 'english')
#         if language == 'hindi':
#             keyboard = [
#                 [
#                     InlineKeyboardButton("SSC", callback_data='category_SSCHi'),
#                     InlineKeyboardButton("RRB", callback_data='category_RRBHi')
#                 ],
#                 [InlineKeyboardButton("Back", callback_data='back_to_languages')]
#             ]
#         elif language == 'english':
#             keyboard = [
#                 [
#                     InlineKeyboardButton("SSC", callback_data='category_SSCEn'),
#                     InlineKeyboardButton("RRB", callback_data='category_RRBEn')
#                 ],
#                 [InlineKeyboardButton("Back", callback_data='back_to_languages')]
#             ]
        
#         reply_markup = InlineKeyboardMarkup(keyboard)
#         query.edit_message_text(text="Please select your category:", reply_markup=reply_markup)

#     elif query.data.startswith('interval_'):
#         interval = int(query.data.split('_')[1])
#         chat_data = load_chat_data(chat_id)
#         chat_data["interval"] = interval
#         save_chat_data(chat_id, chat_data)
        
#         if chat_data.get("active", False):
#             query.edit_message_text(f"Quiz interval updated to {interval} seconds. Applying new interval immediately.")
#             jobs = context.job_queue.jobs()
#             for job in jobs:
#                 if job.context and job.context["chat_id"] == chat_id:
#                     job.schedule_removal()
                    
#             # Send the first quiz immediately and then schedule subsequent quizzes
#         send_quiz_immediately(context, chat_id)
#         context.job_queue.run_repeating(send_quiz, interval=interval, first=interval, context={"chat_id": chat_id, "used_questions": chat_data.get("used_questions", [])})
#         query.edit_message_text(f"Quiz interval updated to {interval} seconds. Starting quiz.")
#         start_quiz(update, context)

#     elif query.data == 'show_leaderboard':
#         chat_id = update.effective_chat.id
#         # Send initial loading message
#         loading_message = context.bot.send_message(chat_id=chat_id, text="Leaderboard is loading...")

#         # Send loading updates in a separate thread
#         def send_loading_messages(message_id):
#             for i in range(2, 4):
#                 time.sleep(1)  # Wait for 1 second before sending the next message
#                 context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=f"Leaderboard is loading...{i}")

#         loading_thread = threading.Thread(target=send_loading_messages, args=(loading_message.message_id,))
#         loading_thread.start()

#         # Fetch and display the leaderboard
#         top_scores = get_top_scores(20)
#         loading_thread.join()  # Wait for the loading messages to finish

#         if not top_scores:
#             context.bot.delete_message(chat_id=chat_id, message_id=loading_message.message_id)
#             update.effective_message.reply_text("🏆 No scores yet! Start playing to appear on the leaderboard.")
#             return

#         # Delete the loading message
#         context.bot.delete_message(chat_id=chat_id, message_id=loading_message.message_id)

#         # Prepare and send the leaderboard message
#         message = "🏆 *Quiz Leaderboard* 🏆\n\n"
#         medals = ["🥇", "🥈", "🥉"]

#         for rank, (user_id, score) in enumerate(top_scores, start=1):
#             try:
#                 user = context.bot.get_chat(int(user_id))
#                 username = f"@{user.username}" if user.username else f"{user.first_name} {user.last_name or ''}"
#             except Exception:
#                 username = f"User {user_id}"

#             rank_display = medals[rank - 1] if rank <= 3 else f"{rank}."
#             message += f"{rank_display}  *{username}* - {score} Points\n\n"

#         update.effective_message.reply_text(message, parse_mode="Markdown")

#     elif query.data == 'show_stats':
#         user_id = str(update.effective_user.id)
#         score = get_user_score(user_id)
#         update.effective_message.reply_text(f"Your current score is: {score} points.")
        
#     elif query.data == 'show_commands':
#         commands_description = """
#         /start - Start the bot and show the main menu
#         /setinterval - Set the interval for quizzes
#         /stopquiz - Stop the current quiz
#         /pause - Pause the current quiz
#         /resume - Resume a paused quiz
#         /leaderboard - Show the leaderboard
#         /stats - Show your current stats
#         """
#         # Inline button to go back to the main menu
#         keyboard = [
#             [InlineKeyboardButton("Back", callback_data='back_to_main_menu')]
#         ]
#         reply_markup = InlineKeyboardMarkup(keyboard)
#         query.edit_message_text(text=f"Available Commands:\n{commands_description}", reply_markup=reply_markup)

#     elif query.data == 'back_to_main_menu':
#         # Inline buttons for main menu
#         keyboard = [
#             [InlineKeyboardButton("Start Quiz", callback_data='start_quiz')],
#             [
#                 InlineKeyboardButton("Leaderboard", callback_data='show_leaderboard'),
#                 InlineKeyboardButton("My Score", callback_data='show_stats')
#             ],
#             [InlineKeyboardButton("Commands", callback_data='show_commands')]
#         ]
#         reply_markup = InlineKeyboardMarkup(keyboard)
#         query.edit_message_text(text="Welcome to the Quiz Bot! Please choose an option:", reply_markup=reply_markup)


# async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     """Handle button clicks from inline keyboards."""
#     query = update.callback_query
#     await query.answer()  # Acknowledge the button click

#     chat_id = str(query.message.chat.id)
#     chat_data = await load_chat_data(chat_id)

#     try:
#         if query.data == 'start_quiz':
#             # Handle Start Quiz button
#             keyboard = [
#                 [
#                     InlineKeyboardButton("Hindi", callback_data='language_hindi'),
#                     InlineKeyboardButton("English", callback_data='language_english')
#                 ]
#             ]
#             reply_markup = InlineKeyboardMarkup(keyboard)
#             await query.edit_message_text(
#                 text="*Please select your language: [Hindi, English]*",
#                 reply_markup=reply_markup,
#                 parse_mode="Markdown"
#             )

#         elif query.data.startswith('language_'):
#             # Handle language selection
#             language = query.data.split('_')[1]
#             chat_data['language'] = language
#             await save_chat_data(chat_id, chat_data)

#             # Display category options based on language
#             if language == 'hindi':
#                 keyboard = [
#                     [
#                         InlineKeyboardButton("SSC", callback_data='category_SSCHi'),
#                         InlineKeyboardButton("RRB", callback_data='category_RRBHi')
#                     ],
#                     [InlineKeyboardButton("Back", callback_data='back_to_languages')]
#                 ]
#             elif language == 'english':
#                 keyboard = [
#                     [
#                         InlineKeyboardButton("SSC", callback_data='category_SSCEn'),
#                         InlineKeyboardButton("RRB", callback_data='category_RRBEn')
#                     ],
#                     [InlineKeyboardButton("Back", callback_data='back_to_languages')]
#                 ]

#             reply_markup = InlineKeyboardMarkup(keyboard)
#             await query.edit_message_text(
#                 text=f"*Language selected: {language.upper()}\nPlease select your category: [SSC, RRB]*",
#                 reply_markup=reply_markup,
#                 parse_mode="Markdown"
#             )

#         elif query.data.startswith('category_'):
#             # Handle category selection
#             category = query.data.split('_')[1]
#             chat_data['category'] = category
#             await save_chat_data(chat_id, chat_data)

#             # Directly start the quiz with interval selection
#             keyboard = [
#                 [
#                     InlineKeyboardButton("30 sec", callback_data='interval_30'),
#                     InlineKeyboardButton("1 min", callback_data='interval_60'),
#                     InlineKeyboardButton("5 min", callback_data='interval_300')
#                 ],
#                 [
#                     InlineKeyboardButton("10 min", callback_data='interval_600'),
#                     InlineKeyboardButton("30 min", callback_data='interval_1800'),
#                     InlineKeyboardButton("60 min", callback_data='interval_3600')
#                 ]
#             ]
#             reply_markup = InlineKeyboardMarkup(keyboard)
#             await query.edit_message_text(
#                 text=f"Category selected: {category.upper()}\n"
#                      f"*Please select the interval for quizzes:*",
#                 reply_markup=reply_markup,
#                 parse_mode="Markdown"
#             )

#         elif query.data.startswith('interval_'):
#             # Handle interval selection
#             interval = int(query.data.split('_')[1])
#             chat_data["interval"] = interval
#             await save_chat_data(chat_id, chat_data)

#             # If quiz is already active, update interval
#             if chat_data.get("active", False):
#                 await query.edit_message_text(f"Quiz interval updated to {interval} seconds. Applying new interval immediately.")
#                 jobs = context.job_queue.jobs()
#                 for job in jobs:
#                     if job.context and job.context["chat_id"] == chat_id:
#                         job.schedule_removal()

#                 # Send the first quiz immediately and schedule subsequent quizzes
#                 await send_quiz_immediately(context, chat_id)
#                 context.job_queue.run_repeating(
#                     send_quiz,
#                     interval=interval,
#                     first=interval,
#                     context={"chat_id": chat_id, "used_questions": chat_data.get("used_questions", [])}
#                 )
#             else:
#                 await query.edit_message_text(f"Quiz interval updated to {interval} seconds. Starting quiz.")
#                 await start_quiz(update, context)

#         elif query.data == 'back_to_languages':
#             # Go back to language selection
#             keyboard = [
#                 [
#                     InlineKeyboardButton("Hindi", callback_data='language_hindi'),
#                     InlineKeyboardButton("English", callback_data='language_english')
#                 ]
#             ]
#             reply_markup = InlineKeyboardMarkup(keyboard)
#             await query.edit_message_text(
#                 text="Please select your language:",
#                 reply_markup=reply_markup
#             )

#         elif query.data == 'show_leaderboard':
#             # Show leaderboard
#             await show_leaderboard(update, context)

#         elif query.data == 'show_stats':
#             # Show user stats
#             user_id = str(update.effective_user.id)
#             score = await get_user_score(user_id)
#             await query.edit_message_text(f"Your current score is: {score} points.")

#         elif query.data == 'show_commands':
#             # Show available commands
#             commands_description = """
#             /start - Start the bot and show the main menu
#             /setinterval - Set the interval for quizzes
#             /stopquiz - Stop the current quiz
#             /pause - Pause the current quiz
#             /resume - Resume a paused quiz
#             /leaderboard - Show the leaderboard
#             /stats - Show your current stats
#             """
#             keyboard = [[InlineKeyboardButton("Back", callback_data='back_to_main_menu')]]
#             reply_markup = InlineKeyboardMarkup(keyboard)
#             await query.edit_message_text(
#                 text=f"Available Commands:\n{commands_description}",
#                 reply_markup=reply_markup
#             )

#         elif query.data == 'back_to_main_menu':
#             # Go back to the main menu
#             keyboard = [
#                 [InlineKeyboardButton("Start Quiz", callback_data='start_quiz')],
#                 [
#                     InlineKeyboardButton("Leaderboard", callback_data='show_leaderboard'),
#                     InlineKeyboardButton("My Score", callback_data='show_stats')
#                 ],
#                 [InlineKeyboardButton("Commands", callback_data='show_commands')]
#             ]
#             reply_markup = InlineKeyboardMarkup(keyboard)
#             await query.edit_message_text(
#                 text="Welcome to the Quiz Bot! Please choose an option:",
#                 reply_markup=reply_markup
#             )

#     except Exception as e:
#         logger.error(f"Error handling button callback: {e}")
#         await query.edit_message_text("An error occurred while processing your request. Please try again later.")
        
# def set_interval(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     chat_id = str(update.effective_chat.id)

#     if not context.args or not context.args[0].isdigit():
#         update.message.reply_text("Usage: /setinterval <seconds>")
#         return
    
#     interval = int(context.args[0])
#     if interval < 10:
#         update.message.reply_text("Interval must be at least 10 seconds.")
#         return

#     chat_data = load_chat_data(chat_id)
#     chat_data["interval"] = interval
#     save_chat_data(chat_id, chat_data)

#     # If quiz is already running, update the interval immediately
#     if chat_data.get("active", False):
#         update.message.reply_text(f"Quiz interval updated to {interval} seconds. Applying new interval immediately.")
#         jobs = context.job_queue.jobs()
#         for job in jobs:
#             if job.context and job.context["chat_id"] == chat_id:
#                 job.schedule_removal()
#         # Send the first quiz immediately and then schedule subsequent quizzes
#         send_quiz_immediately(context, chat_id)
#         context.job_queue.run_repeating(send_quiz, interval=interval, first=interval, context={"chat_id": chat_id, "used_questions": chat_data.get("used_questions", [])})
#     else:
#         update.message.reply_text(f"Quiz interval updated to {interval} seconds.")
#         start_quiz(update, context)
async def set_interval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)

    # Validate the argument
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text("Usage: /setinterval <seconds>")
        return

    interval = int(context.args[0])
    if interval < 10:
        await update.message.reply_text("Interval must be at least 10 seconds.")
        return

    # Load chat data and update the interval
    chat_data = await load_chat_data(chat_id)
    chat_data["interval"] = interval
    await save_chat_data(chat_id, chat_data)

    # Check if an active quiz is running
    if chat_data.get("active", False):
        await update.message.reply_text(f"Quiz interval updated to {interval} seconds. Applying new interval immediately.")
        
        # Remove existing jobs for this chat
        jobs = context.job_queue.jobs()
        for job in jobs:
            if job.context and job.context["chat_id"] == chat_id:
                job.schedule_removal()

        # Send the first quiz immediately and schedule subsequent quizzes
        await send_quiz_immediately(context, chat_id)
        context.job_queue.run_repeating(
            send_quiz, 
            interval=interval, 
            first=interval, 
            context={"chat_id": chat_id, "used_questions": chat_data.get("used_questions", [])}
        )
    else:
        await update.message.reply_text(f"Quiz interval updated to {interval} seconds.")
        await start_quiz(update, context)

# def start_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     chat_id = str(update.effective_chat.id)
#     chat_data = load_chat_data(chat_id)

#     today = datetime.now().date().isoformat()  # Convert date to string
#     quizzes_sent = quizzes_sent_collection.find_one({"chat_id": chat_id, "date": today})

#     # if quizzes_sent and quizzes_sent.get("count", 0) >= 10:
#     #     update.message.reply_text("You have reached your daily limit. The next quiz will be sent tomorrow.")
#     #     return

#     if chat_data.get("active", False):
#         update.message.reply_text("A quiz is already running in this chat!")
#         return

#     interval = chat_data.get("interval", 30)  # Default interval to 30 seconds if not set
#     chat_data["active"] = True
#     save_chat_data(chat_id, chat_data)

#     update.message.reply_text(f"Quiz started! Interval: {interval} seconds.")

#     # Send the first quiz immediately
#     send_quiz_immediately(context, chat_id)

#     # Schedule subsequent quizzes at the specified interval
#     context.job_queue.run_repeating(send_quiz, interval=interval, first=interval, context={"chat_id": chat_id, "used_questions": []})
async def start_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)

    # Load chat data asynchronously
    chat_data = await load_chat_data(chat_id)

    # Check if a quiz is already running
    if chat_data.get("active", False):
        await update.message.reply_text("A quiz is already running in this chat!")
        return

    # Default interval to 30 seconds if not set
    interval = chat_data.get("interval", 30)
    chat_data["active"] = True
    await save_chat_data(chat_id, chat_data)

    # Notify the user that the quiz has started
    await update.message.reply_text(f"Quiz started! Interval: {interval} seconds.")

    # Send the first quiz immediately
    await send_quiz_immediately(context, chat_id)

    # Schedule subsequent quizzes at the specified interval
    context.job_queue.run_repeating(
        send_quiz,
        interval=interval,
        first=interval,
        context={"chat_id": chat_id, "used_questions": chat_data.get("used_questions", [])}
    )
def stop_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    chat_data = load_chat_data(chat_id)

    if chat_data:
        chat_data["active"] = False
        save_chat_data(chat_id, chat_data)

        jobs = context.job_queue.jobs()
        for job in jobs:
            if job.context and job.context["chat_id"] == chat_id:
                job.schedule_removal()

        update.message.reply_text("Quiz stopped successfully.")
    else:
        update.message.reply_text("No active quiz to stop.")

def pause_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    chat_data = load_chat_data(chat_id)

    if not chat_data.get("active", False):
        update.message.reply_text("No active quiz to pause.")
        return

    chat_data["paused"] = True
    save_chat_data(chat_id, chat_data)

    jobs = context.job_queue.jobs()
    for job in jobs:
        if job.context and job.context["chat_id"] == chat_id:
            job.schedule_removal()

    update.message.reply_text("Quiz paused successfully.")

def resume_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    chat_data = load_chat_data(chat_id)

    if not chat_data.get("paused", False):
        update.message.reply_text("No paused quiz to resume.")
        return

    chat_data["paused"] = False
    save_chat_data(chat_id, chat_data)

    interval = chat_data.get("interval", 30)
    context.job_queue.run_repeating(send_quiz, interval=interval, first=0, context={"chat_id": chat_id, "used_questions": []})

    update.message.reply_text("Quiz resumed successfully.")
    
async def restart_active_quizzes(application):
    active_quizzes = get_active_quizzes()  # This returns an AsyncIOMotorCursor

    async for quiz in active_quizzes:  # Use async for to iterate over the cursor
        chat_id = quiz["chat_id"]
        interval = quiz["data"].get("interval", 30)
        used_questions = quiz["data"].get("used_questions", [])

        # Check if bot is still a member of the chat
        try:
            await application.bot.get_chat_member(chat_id, application.bot.id)  # Await the async call
        except Exception:
            logger.warning(f"Bot is no longer a member of chat {chat_id}. Removing from active quizzes.")
            await save_chat_data(chat_id, {"active": False})  # Mark chat as inactive
            continue

        logger.info(f"Restarting quiz for chat_id: {chat_id} with interval {interval} seconds.")
        application.job_queue.run_repeating(
            send_quiz,
            interval=interval,
            first=0,
            data={"chat_id": chat_id, "used_questions": used_questions}
        )

def check_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    score = get_user_score(user_id)
    update.message.reply_text(f"Your current score is: {score} points.")

# def show_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     chat_id = update.message.chat_id

#     # Send initial loading message
#     loading_message = context.bot.send_message(chat_id=chat_id, text="Leaderboard is loading...")

#     # Send loading updates in a separate thread
#     def send_loading_messages(message_id):
#         for i in range(2, 4):
#             time.sleep(1)  # Wait for 1 second before sending the next message
#             context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=f"Leaderboard is loading...{i}")

#     loading_thread = threading.Thread(target=send_loading_messages, args=(loading_message.message_id,))
#     loading_thread.start()

#     # Fetch and display the leaderboard
#     top_scores = get_top_scores(20)
#     loading_thread.join()  # Wait for the loading messages to finish

#     if not top_scores:
#         context.bot.delete_message(chat_id=chat_id, message_id=loading_message.message_id)
#         update.message.reply_text("🏆 No scores yet! Start playing to appear on the leaderboard.")
#         return

#     # Delete the loading message
#     context.bot.delete_message(chat_id=chat_id, message_id=loading_message.message_id)

#     # Prepare and send the leaderboard message
#     message = "🏆 *Quiz Leaderboard* 🏆\n\n"
#     medals = ["🥇", "🥈", "🥉"]

#     for rank, (user_id, score) in enumerate(top_scores, start=1):
#         try:
#             user = context.bot.get_chat(int(user_id))
#             username = f"@{user.username}" if user.username else f"{user.first_name} {user.last_name or ''}"
#         except Exception:
#             username = f"User {user_id}"

#         rank_display = medals[rank - 1] if rank <= 3 else f"{rank}."
#         message += f"{rank_display}  *{username}* - {score} Points\n\n"

#     update.message.reply_text(message, parse_mode="Markdown")
async def show_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    logger.info("Fetching leaderboard...")

    try:
        loading_message = await context.bot.send_message(chat_id=chat_id, text="Leaderboard is loading...")

        # Fetch scores
        top_scores = await get_top_scores(20)
        if not top_scores:
            logger.info("No scores found for leaderboard.")
            await context.bot.delete_message(chat_id=chat_id, message_id=loading_message.message_id)
            await update.message.reply_text("🏆 No scores yet! Start playing to appear on the leaderboard.")
            return

        # Prepare leaderboard message
        message = "🏆 *Quiz Leaderboard* 🏆\n\n"
        medals = ["🥇", "🥈", "🥉"]

        for rank, (user_id, score) in enumerate(top_scores, start=1):
            try:
                user = await context.bot.get_chat(int(user_id))
                username = f"@{user.username}" if user.username else f"{user.first_name} {user.last_name or ''}"
            except Exception as e:
                logger.error(f"Error fetching user data for user_id {user_id}: {e}")
                username = f"User {user_id}"

            rank_display = medals[rank - 1] if rank <= 3 else f"{rank}."
            message += f"{rank_display}  *{username}* - {score} Points\n\n"

        # Send leaderboard message
        await context.bot.delete_message(chat_id=chat_id, message_id=loading_message.message_id)
        await update.message.reply_text(message, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Error while loading leaderboard: {e}")
        await update.message.reply_text("An error occurred while loading the leaderboard. Please try again later.")

async def get_top_scores(limit=20):
    try:
        # Replace this with your actual database query logic
        scores = await db.scores.find().sort("score", -1).limit(limit).to_list(length=limit)
        return [(score["user_id"], score["score"]) for score in scores]
    except Exception as e:
        logger.error(f"Error fetching top scores: {e}")
        return []

def next_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    chat_data = load_chat_data(chat_id)

    # Check if there are any active quizzes
    if not chat_data.get("active", False):
        update.message.reply_text("No active quiz. Use /start to begin a quiz session.")
        return

    # Send the next quiz immediately
    send_quiz_immediately(context, chat_id)
    update.message.reply_text("Next quiz has been sent!")


def main():
    # Create the Application instance
    application = Application.builder().token(TOKEN).build()
    # Create an AsyncIO event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Test MongoDB connection
    loop.run_until_complete(test_mongo_connection())

    # Add handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("setinterval", set_interval))
    application.add_handler(CommandHandler("stopquiz", stop_quiz))
    application.add_handler(CommandHandler("pause", pause_quiz))
    application.add_handler(CommandHandler("resume", resume_quiz))
    application.add_handler(CommandHandler("next", next_quiz))
    application.add_handler(CallbackQueryHandler(button))
    application.add_handler(PollAnswerHandler(handle_poll_answer))
    application.add_handler(CommandHandler("leaderboard", show_leaderboard))
    application.add_handler(CommandHandler("broadcast", broadcast))
    application.add_handler(CommandHandler("stats", check_stats))

    # # Schedule async tasks
    # asyncio.run(restart_active_quizzes(application))
   # Schedule the restart_active_quizzes coroutine
    async def initialize_quizzes():
        await restart_active_quizzes(application)

    application.job_queue.run_once(lambda _: application.create_task(initialize_quizzes()), 0)


    # Start the bot
    application.run_polling()


if __name__ == '__main__':
    main()




# def main():
#     updater = Updater(TOKEN, use_context=True)
#     dp = updater.dispatcher
    
#     dp.add_handler(CommandHandler("start", start_command))
#     dp.add_handler(CommandHandler("setinterval", set_interval))
#     dp.add_handler(CommandHandler("stopquiz", stop_quiz))
#     dp.add_handler(CommandHandler("pause", pause_quiz))
#     dp.add_handler(CommandHandler("resume", resume_quiz))
#     dp.add_handler(CommandHandler("next", next_quiz))
#     dp.add_handler(CallbackQueryHandler(button))
#     dp.add_handler(PollAnswerHandler(handle_poll_answer))
#     dp.add_handler(CommandHandler("leaderboard", show_leaderboard))
#     dp.add_handler(CommandHandler("broadcast", broadcast))
#     dp.add_handler(CommandHandler("stats", check_stats))



    
#     updater.start_polling()
#      # Schedule restart_active_quizzes asynchronously
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(restart_active_quizzes(updater))

#     updater.idle()

# if __name__ == '__main__':
#     main()
