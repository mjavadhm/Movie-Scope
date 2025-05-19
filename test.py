from services.movie_service import fetch_and_save_movie, fetch_and_save_upcoming_movies
from models import get_session
import asyncio
async def some_handler():
    async for session in get_session():
        movie = await fetch_and_save_upcoming_movies(session)#, tmdb_id=329865)
        # if movie:
        #     print(f"فیلم {movie.title} ذخیره شد.")
        # else:
        #     print("مشکل در ذخیره‌سازی یا فیلم قبلاً وجود داشت.")


asyncio.run(some_handler())