from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from config import DATABASE_URL

# Create async engine
engine = create_async_engine(DATABASE_URL)

# Create async session factory
async_session = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

# Create declarative base
Base = declarative_base()

# Function to get DB session
async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session
