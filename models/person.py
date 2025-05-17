from sqlalchemy import Column, Integer, Text
from sqlalchemy.orm import relationship
from . import Base

class Person(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    tmdb_id = Column(Integer, unique=True, nullable=False)
    name = Column(Text, nullable=False)
    profile_url = Column(Text)
    known_for_department = Column(Text)

    # Relationships
    cast_movies = relationship("MovieCast", back_populates="person", cascade="all, delete-orphan")
    crew_movies = relationship("MovieCrew", back_populates="person", cascade="all, delete-orphan")
