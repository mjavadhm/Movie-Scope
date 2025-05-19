from aiogram import Router, F
from aiogram.types import CallbackQuery
from sqlalchemy import select, update
from models import get_session
from models.movie import Movie
from logger import get_logger

# Initialize router
router = Router(name="callbacks")
logger = get_logger()

@router.callback_query(F.data.startswith("button_"))
async def process_button_press(callback: CallbackQuery):
    """Handle button presses"""
    try:
        if callback.data:
            # Get the button data (everything after "button_")
            button_data = callback.data.replace("button_", "")
            
            # Answer the callback to remove loading state
            await callback.answer()
            
            # Handle different button actions
            if button_data == "example" and callback.message:
                await callback.message.edit_text(
                    "You pressed the example button!",
                    reply_markup=None  # Remove the inline keyboard
                )
    except Exception as e:
        logger.error(f"Error in callback handler: {e}", exc_info=True)
        await callback.answer("An error occurred", show_alert=True)

@router.callback_query(F.data.startswith("follow_movie_"))
async def follow_movie_callback(callback: CallbackQuery):
    """Handle follow movie button press"""
    try:
        # Extract movie TMDB ID from callback data
        tmdb_id = int(callback.data.replace("follow_movie_", ""))
        
        # Get database session
        async for session in get_session():
            # Find the movie by TMDB ID
            result = await session.execute(
                select(Movie).where(Movie.tmdb_id == tmdb_id)
            )
            movie = result.scalar_one_or_none()
            
            if movie:
                if movie.is_tracked:
                    # Movie is already being tracked
                    await callback.answer(
                        f"✅ '{movie.title}' is already being followed!",
                        show_alert=True
                    )
                else:
                    # Mark movie as tracked
                    await session.execute(
                        update(Movie)
                        .where(Movie.tmdb_id == tmdb_id)
                        .values(is_tracked=True)
                    )
                    await session.commit()
                    
                    # Update button to show it's being followed
                    if callback.message and callback.message.reply_markup:
                        # Create updated keyboard with different button text
                        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                        updated_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(
                                text="✅ Following", 
                                callback_data=f"unfollow_movie_{tmdb_id}"
                            )]
                        ])
                        
                        try:
                            await callback.message.edit_reply_markup(reply_markup=updated_keyboard)
                        except Exception as e:
                            logger.warning(f"Could not update button for movie {tmdb_id}: {e}")
                    
                    await callback.answer(
                        f"🔔 Now following '{movie.title}'!\nYou'll get updates about this movie.",
                        show_alert=True
                    )
                    logger.info(f"User started following movie: {movie.title} (TMDB ID: {tmdb_id})")
            else:
                await callback.answer(
                    "❌ Movie not found in database",
                    show_alert=True
                )
                
    except ValueError:
        logger.error(f"Invalid movie ID in callback: {callback.data}")
        await callback.answer("❌ Invalid movie ID", show_alert=True)
    except Exception as e:
        logger.error(f"Error in follow movie callback: {e}", exc_info=True)
        await callback.answer("❌ An error occurred", show_alert=True)

@router.callback_query(F.data.startswith("unfollow_movie_"))
async def unfollow_movie_callback(callback: CallbackQuery):
    """Handle unfollow movie button press"""
    try:
        # Extract movie TMDB ID from callback data
        tmdb_id = int(callback.data.replace("unfollow_movie_", ""))
        
        # Get database session
        async for session in get_session():
            # Find the movie by TMDB ID
            result = await session.execute(
                select(Movie).where(Movie.tmdb_id == tmdb_id)
            )
            movie = result.scalar_one_or_none()
            
            if movie:
                if not movie.is_tracked:
                    # Movie is not being tracked
                    await callback.answer(
                        f"'{movie.title}' is not being followed",
                        show_alert=True
                    )
                else:
                    # Unmark movie as tracked
                    await session.execute(
                        update(Movie)
                        .where(Movie.tmdb_id == tmdb_id)
                        .values(is_tracked=False)
                    )
                    await session.commit()
                    
                    # Update button back to follow
                    if callback.message and callback.message.reply_markup:
                        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                        updated_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(
                                text="🔔 Follow Movie", 
                                callback_data=f"follow_movie_{tmdb_id}"
                            )]
                        ])
                        
                        try:
                            await callback.message.edit_reply_markup(reply_markup=updated_keyboard)
                        except Exception as e:
                            logger.warning(f"Could not update button for movie {tmdb_id}: {e}")
                    
                    await callback.answer(
                        f"❌ Unfollowed '{movie.title}'",
                        show_alert=True
                    )
                    logger.info(f"User unfollowed movie: {movie.title} (TMDB ID: {tmdb_id})")
            else:
                await callback.answer(
                    "❌ Movie not found in database",
                    show_alert=True
                )
                
    except ValueError:
        logger.error(f"Invalid movie ID in callback: {callback.data}")
        await callback.answer("❌ Invalid movie ID", show_alert=True)
    except Exception as e:
        logger.error(f"Error in unfollow movie callback: {e}", exc_info=True)
        await callback.answer("❌ An error occurred", show_alert=True)