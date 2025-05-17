from sqlalchemy import Column, Integer, Text, ForeignKey
from sqlalchemy.orm import relationship
from . import Base

class MovieCrew(Base):
    __tablename__ = 'movie_crew'

    id = Column(Integer, primary_key=True)
    movie_id = Column(Integer, ForeignKey('movies.id', ondelete="CASCADE"))
    person_id = Column(Integer, ForeignKey('people.id', ondelete="CASCADE"))
    job = Column(Text)
    department = Column(Text)

    # Relationships
    movie = relationship("Movie", back_populates="crew")
    person = relationship("Person", back_populates="crew_movies")
