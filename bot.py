from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from config import BOT_TOKEN

# Initialize bot and dispatcher
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN must be set")
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

# Function to get bot instance
def get_bot() -> Bot:
    return bot

# Function to get dispatcher instance
def get_dispatcher() -> Dispatcher:
    return dp
