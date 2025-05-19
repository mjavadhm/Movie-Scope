from os import getenv
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Bot settings
BOT_TOKEN = getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not set in .env")

# Error logging channel
ERROR_CHANNEL_ID = getenv("ERROR_CHANNEL_ID")
if not ERROR_CHANNEL_ID:
    raise ValueError("ERROR_CHANNEL_ID not set in .env")
ERROR_CHANNEL_ID = int(ERROR_CHANNEL_ID)

# Database settings
DATABASE_URL = getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set in .env")

TMDB_API_KEY = getenv("TMDB_API_KEY")
if not TMDB_API_KEY:
    raise ValueError("TMDB_API_KEY not set in .env")

MOVIES_CHANNEL_ID = getenv("MOVIES_CHANNEL_ID")
if not MOVIES_CHANNEL_ID:
    raise ValueError("MOVIES_CHANNEL_ID not set in .env")
MOVIES_CHANNEL_ID = int(MOVIES_CHANNEL_ID)