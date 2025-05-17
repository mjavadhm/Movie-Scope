from sqlalchemy import Column, Integer, Text, ForeignKey
from sqlalchemy.orm import relationship
from . import Base

class MovieCast(Base):
    __tablename__ = 'movie_cast'

    id = Column(Integer, primary_key=True)
    movie_id = Column(Integer, ForeignKey('movies.id', ondelete="CASCADE"))
    person_id = Column(Integer, ForeignKey('people.id', ondelete="CASCADE"))
    character_name = Column(Text)
    cast_order = Column(Integer)

    # Relationships
    movie = relationship("Movie", back_populates="cast")
    person = relationship("Person", back_populates="cast_movies")
