import asyncio
import logging
from aiogram.types import BotCommand
from bot import dp, bot
from routers import commands_router, callbacks_router, messages_router
from services.movie_scheduler import MovieUpdateScheduler
from config import MOVIES_CHANNEL_ID
from logger import get_logger

# Get logger
logger = get_logger()

# Register routers
dp.include_router(commands_router)
dp.include_router(callbacks_router)
dp.include_router(messages_router)

# Global scheduler instance
scheduler = None

async def set_commands():
    """Set bot commands in menu"""
    commands = [
        BotCommand(command="start", description="Start the bot"),
        BotCommand(command="help", description="Show help message"),
        BotCommand(command="post_upcoming", description="Post upcoming movies"),
        BotCommand(command="check_updates", description="Check movie updates"),
    ]
    await bot.set_my_commands(commands)

async def start_scheduler():
    """Start the movie update scheduler"""
    global scheduler
    scheduler = MovieUpdateScheduler(MOVIES_CHANNEL_ID)
    # Start scheduler in background
    asyncio.create_task(scheduler.start_scheduler())
    logger.info("Movie update scheduler started in background")

async def main():
    """Main function"""
    # Set logging level
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting bot...")

    try:
        # Set bot commands
        await set_commands()
        
        # Start the movie update scheduler
        await start_scheduler()
        
        # Start polling
        logger.info("Bot is running...")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Critical error: {e}", exc_info=True)
    finally:
        # Stop scheduler if running
        if scheduler:
            scheduler.stop_scheduler()
        logger.info("Bot stopped")

if __name__ == "__main__":
    asyncio.run(main())