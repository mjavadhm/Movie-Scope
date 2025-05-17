from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import Message
from logger import get_logger

# Initialize router
router = Router(name="commands")
logger = get_logger()

@router.message(Command("start"))
async def cmd_start(message: Message):
    """Handle /start command"""
    try:
        await message.answer(
            "👋 Welcome! I'm your bot.\n"
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
            "/help - Show this help message"
        )
        await message.answer(help_text)
    except Exception as e:
        logger.error(f"Error in help command: {e}", exc_info=True)
