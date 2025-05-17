from aiogram import Router, F
from aiogram.types import CallbackQuery
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
