from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message
from models import get_session
from services.movie_service import fetch_and_save_upcoming_movies
from services.channel_services import ChannelService
from config import MOVIES_CHANNEL_ID
from logger import get_logger

# Initialize router
router = Router(name="commands")
logger = get_logger()

@router.message(Command("start"))
async def cmd_start(message: Message):
    """Handle /start command"""
    try:
        await message.answer(
            "👋 Welcome! I'm your movie bot.\n"
            "Use /help to see available commands."
        )
    except Exception as e:
        logger.error(f"Error in start command: {e}", exc_info=True)

@router.message(Command("help"))
async def cmd_help(message: Message):
    """Handle /help command"""
    try:
        help_text = (
            "Available commands:\n"
            "/start - Start the bot\n"
            "/help - Show this help message\n"
            "/post_upcoming - Post upcoming movies to channel (Admin only)\n"
            "/check_updates - Force check for movie updates (Admin only)"
        )
        await message.answer(help_text)
    except Exception as e:
        logger.error(f"Error in help command: {e}", exc_info=True)

@router.message(Command("post_upcoming"))
async def cmd_post_upcoming(message: Message):
    """Handle /post_upcoming command - fetch and post upcoming movies"""
    try:
        # Check if user is authorized (you might want to add admin check here)
        user_id = message.from_user.id if message.from_user else None
        
        await message.answer("🔄 Fetching upcoming movies...")
        
        # Fetch upcoming movies
        async for session in get_session():
            movies = await fetch_and_save_upcoming_movies(session, page=1, limit=10)
            
            if not movies:
                await message.answer("❌ No new upcoming movies found")
                return
            
            # Send status message
            await message.answer(
                f"✅ Found {len(movies)} new upcoming movies!\n"
                f"🔄 Posting to channel..."
            )
            
            # Post to channel
            channel_service = ChannelService(MOVIES_CHANNEL_ID)
            sent_count, failed_count = await channel_service.send_bulk_movies(movies)
            
            # Send summary
            summary = (
                f"📊 <b>Posting Summary:</b>\n"
                f"✅ Successfully posted: {sent_count}\n"
                f"❌ Failed to post: {failed_count}\n"
                f"📺 Channel: Movies Channel"
            )
            await message.answer(summary, parse_mode="HTML")
            
    except Exception as e:
        logger.error(f"Error in post upcoming command: {e}", exc_info=True)
        await message.answer("❌ An error occurred while posting movies")

@router.message(Command("check_updates"))
async def cmd_check_updates(message: Message):
    """Handle /check_updates command - force check for movie updates"""
    try:
        from services.movie_scheduler import MovieUpdateScheduler
        
        await message.answer("🔄 Checking for movie updates...")
        
        # Create scheduler and force check
        scheduler = MovieUpdateScheduler(MOVIES_CHANNEL_ID)
        await scheduler.force_check_now()
        
        await message.answer("✅ Movie update check completed!")
        
    except Exception as e:
        logger.error(f"Error in check updates command: {e}", exc_info=True)
        await message.answer("❌ An error occurred while checking for updates")        