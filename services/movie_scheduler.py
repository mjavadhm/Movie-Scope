import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import List, Dict, Any
from sqlalchemy import select
from models import get_session
from models.movie import Movie
from services.channel_services import ChannelService
from logger import get_logger
from config import TMDB_API_KEY

logger = get_logger()

class MovieUpdateScheduler:
    def __init__(self, channel_id: int):
        self.channel_service = ChannelService(channel_id)
        self.is_running = False
        self.check_interval = 24 * 60 * 60  # 24 hours in seconds
        
    async def start_scheduler(self):
        """Start the movie update scheduler"""
        self.is_running = True
        logger.info("Movie update scheduler started")
        
        while self.is_running:
            try:
                await self.check_tracked_movies()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}", exc_info=True)
                # Wait 1 hour before retrying if there's an error
                await asyncio.sleep(3600)
    
    def stop_scheduler(self):
        """Stop the movie update scheduler"""
        self.is_running = False
        logger.info("Movie update scheduler stopped")
    
    async def check_tracked_movies(self):
        """Check all tracked movies for updates"""
        logger.info("Starting daily check for tracked movies...")
        
        async for session in get_session():
            # Get all tracked movies
            result = await session.execute(
                select(Movie).where(Movie.is_tracked == True)
            )
            tracked_movies = result.scalars().all()
            
            if not tracked_movies:
                logger.info("No tracked movies found")
                return
            
            logger.info(f"Checking {len(tracked_movies)} tracked movies for updates")
            
            updates_found = 0
            for movie in tracked_movies:
                try:
                    has_updates = await self.check_movie_updates(movie)
                    if has_updates:
                        updates_found += 1
                        await asyncio.sleep(2)  # Rate limiting
                except Exception as e:
                    logger.error(f"Error checking movie {movie.title}: {e}")
            
            # Send summary if any updates were found
            if updates_found > 0:
                summary_text = (
                    f"📱 <b>Daily Movie Update Summary</b>\n\n"
                    f"Found updates for {updates_found} tracked movie(s)!\n"
                    f"Check the messages above for details."
                )
                await self.channel_service.send_status_message(summary_text)
            else:
                logger.info("No updates found for tracked movies")
    
    async def check_movie_updates(self, movie: Movie) -> bool:
        """
        Check if a specific movie has updates
        Returns True if updates were found and sent
        """
        try:
            # Get latest movie data from TMDB
            latest_data = await self.fetch_movie_from_tmdb(movie.tmdb_id)
            if not latest_data:
                return False
            
            # Check for significant changes
            updates = []
            
            # Check release date changes
            if latest_data.get("release_date") != str(movie.release_date):
                old_date = movie.release_date.strftime("%Y-%m-%d") if movie.release_date else "Unknown"
                new_date = latest_data.get("release_date", "Unknown")
                updates.append(f"📅 Release date changed: {old_date} → {new_date}")
            
            # Check rating changes (significant change = more than 0.5 points)
            if movie.vote_average and latest_data.get("vote_average"):
                rating_diff = abs(latest_data["vote_average"] - movie.vote_average)
                if rating_diff >= 0.5:
                    updates.append(
                        f"⭐ Rating updated: {movie.vote_average}/10 → {latest_data['vote_average']}/10"
                    )
            
            # Check popularity changes (significant change = more than 20%)
            if movie.popularity and latest_data.get("popularity"):
                popularity_change = (latest_data["popularity"] - movie.popularity) / movie.popularity
                if abs(popularity_change) >= 0.2:  # 20% change
                    direction = "📈" if popularity_change > 0 else "📉"
                    updates.append(
                        f"{direction} Popularity changed: {movie.popularity:.1f} → {latest_data['popularity']:.1f}"
                    )
            
            # Send update notification if changes found
            if updates:
                update_text = (
                    f"🔔 <b>Movie Update: {movie.title}</b>\n\n"
                    f"{''.join(f'{update}\n' for update in updates)}\n"
                    f"<i>Last checked: {datetime.now().strftime('%Y-%m-%d %H:%M')}</i>"
                )
                
                await self.channel_service.send_status_message(update_text)
                logger.info(f"Sent update notification for movie: {movie.title}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking updates for movie {movie.title}: {e}")
            return False
    
    async def fetch_movie_from_tmdb(self, tmdb_id: int) -> Dict[str, Any]:
        """Fetch movie data from TMDB API"""
        url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
        params = {"api_key": TMDB_API_KEY}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        logger.warning(f"TMDB API returned status {response.status} for movie {tmdb_id}")
                        return None
        except Exception as e:
            logger.error(f"Error fetching movie {tmdb_id} from TMDB: {e}")
            return None
    
    async def force_check_now(self):
        """Force an immediate check of all tracked movies (for testing/manual trigger)"""
        logger.info("Force checking tracked movies...")
        await self.check_tracked_movies()