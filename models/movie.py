from sqlalchemy import Column, Integer, Text, Date, Float, Boolean, TIMESTAMP
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import relationship
from . import Base

class Movie(Base):
    __tablename__ = 'movies'

    id = Column(Integer, primary_key=True)
    tmdb_id = Column(Integer, unique=True, nullable=False)
    title = Column(Text, nullable=False)
    overview = Column(Text)
    release_date = Column(Date)
    popularity = Column(Float)
    vote_average = Column(Float)
    genres = Column(ARRAY(Text))
    poster_url = Column(Text)
    is_tracked = Column(Boolean, default=False)
    created_at = Column(TIMESTAMP, server_default='CURRENT_TIMESTAMP')

    # Relationships
    cast = relationship("MovieCast", back_populates="movie", cascade="all, delete-orphan")
    crew = relationship("MovieCrew", back_populates="movie", cascade="all, delete-orphan")
