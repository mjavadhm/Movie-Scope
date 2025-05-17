import logging
import asyncio
from aiogram import Bot
from config import BOT_TOKEN, ERROR_CHANNEL_ID

# Create logger
logger = logging.getLogger('bot_logger')
logger.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_format)
logger.addHandler(console_handler)

# Custom handler for sending errors to Telegram channel
class TelegramBotHandler(logging.Handler):
    def __init__(self, bot: Bot, channel_id: int):
        super().__init__()
        self.bot = bot
        self.channel_id = channel_id
        
    def emit(self, record):
        log_entry = self.format(record)
        asyncio.create_task(self._send_log_entry(log_entry))
        
    async def _send_log_entry(self, log_entry: str):
        try:
            await self.bot.send_message(
                chat_id=self.channel_id,
                text=f"🔴 Error Log:\n\n{log_entry}"
            )
        except Exception as e:
            # Fallback to console if sending to Telegram fails
            print(f"Failed to send log to Telegram: {e}")

# Initialize bot instance for logger
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN must be set for logging")
bot = Bot(token=BOT_TOKEN)

# Add Telegram handler for ERROR level
telegram_handler = TelegramBotHandler(bot, ERROR_CHANNEL_ID)
telegram_handler.setLevel(logging.ERROR)
telegram_format = logging.Formatter('%(levelname)s - %(asctime)s\n\n%(message)s')
telegram_handler.setFormatter(telegram_format)
logger.addHandler(telegram_handler)

# Function to get logger
def get_logger():
    return logger
