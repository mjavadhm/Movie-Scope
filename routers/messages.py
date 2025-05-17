from aiogram import Router, F
from aiogram.types import Message
from logger import get_logger

# Initialize router
router = Router(name="messages")
logger = get_logger()

@router.message(F.text)
async def handle_text(message: Message):
    """Handle text messages"""
    try:
        # Echo the message back
        await message.reply(
            f"You said: {message.text}\n"
            "I'm a bot template and can be customized to handle messages differently!"
        )
    except Exception as e:
        logger.error(f"Error in message handler: {e}", exc_info=True)
