from datetime import datetime
import tmdbsimple as tmdb
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from models.movie import Movie
from models.person import Person
from models.movie_cast import MovieCast
from models.movie_crew import MovieCrew

# تنظیم API key
from config import TMDB_API_KEY
tmdb.API_KEY = TMDB_API_KEY


async def fetch_and_save_upcoming_movies(session, page=1, limit=None):
    movies_api = tmdb.Movies()
    response = movies_api.upcoming(page=page)

    results = response.get("results", [])
    if limit:
        results = results[:limit]

    saved_movies = []
    for item in results:
        try:
            movie = await fetch_and_save_movie(session, tmdb_id=item["id"])
            if movie:
                saved_movies.append(movie)
        except Exception as e:
            print(f"❌ خطا در ذخیره فیلم {item['id']}: {e}")
    return saved_movies

async def get_existing_tmdb_ids(session, tmdb_ids: list[int]) -> set[int]:
    """
    تمام فیلم‌هایی که tmdb_id آن‌ها در لیست هست و در دیتابیس ذخیره شده‌اند را می‌گیرد.
    """
    result = await session.execute(
        select(Movie.tmdb_id).where(Movie.tmdb_id.in_(tmdb_ids))
    )
    return {row[0] for row in result.all()}

async def fetch_and_save_upcoming_movies(session, page=1, limit=None):
    movies_api = tmdb.Movies()
    response = movies_api.upcoming(page=page)

    results = response.get("results", [])
    if limit:
        results = results[:limit]

    tmdb_ids = [movie["id"] for movie in results]
    existing_ids = await get_existing_tmdb_ids(session, tmdb_ids)

    saved_movies = []
    for item in results:
        # print(item)
        if item["id"] in existing_ids:
            continue  # از قبل ذخیره شده

        try:
            movie = await fetch_and_save_movie(session, tmdb_id=item["id"])
            if movie:
                saved_movies.append(movie)
        except Exception as e:
            print(f"❌ خطا در ذخیره فیلم {item['id']}: {e}")
    return saved_movies


async def get_or_create_person(session, person_data):
    """
    بر اساس tmdb_id یک Person را یا پیدا می‌کند یا می‌سازد.
    """
    result = await session.execute(
        select(Person).where(Person.tmdb_id == person_data["id"])
    )
    person = result.scalar_one_or_none()
    if person:
        return person

    person = Person(
        tmdb_id=person_data["id"],
        name=person_data["name"],
        profile_url=person_data.get("profile_path"),
        known_for_department=person_data.get("known_for_department"),
    )
    session.add(person)
    await session.flush()
    return person


async def save_movie_with_cast_and_crew(session, movie_data, cast_list, crew_list):
    """
    ذخیره فیلم به همراه بازیگران و عوامل در دیتابیس
    """
    # اگر قبلاً ذخیره شده، برگردون
    result = await session.execute(
        select(Movie).where(Movie.tmdb_id == movie_data["tmdb_id"])
    )
    existing = result.scalar_one_or_none()
    if existing:
        return existing

    # ساخت شیء Movie
    movie = Movie(
        tmdb_id=movie_data["tmdb_id"],
        title=movie_data["title"],
        overview=movie_data.get("overview"),
        release_date=movie_data.get("release_date"),
        popularity=movie_data.get("popularity"),
        vote_average=movie_data.get("vote_average"),
        genres=movie_data.get("genres", []),
        poster_url=movie_data.get("poster_url"),
    )
    session.add(movie)
    await session.flush()  # تا movie.id تولید بشه

    # اضافه کردن بازیگران
    for c in cast_list:
        person = await get_or_create_person(session, c)
        cast_entry = MovieCast(
            movie_id=movie.id,
            person_id=person.id,
            character_name=c.get("character"),
            cast_order=c.get("order"),
        )
        session.add(cast_entry)

    # اضافه کردن عوامل
    for c in crew_list:
        person = await get_or_create_person(session, c)
        crew_entry = MovieCrew(
            movie_id=movie.id,
            person_id=person.id,
            job=c.get("job"),
            department=c.get("department"),
        )
        session.add(crew_entry)

    try:
        await session.commit()
        return movie
    except IntegrityError:
        await session.rollback()
        return None


async def fetch_and_save_movie(session, tmdb_id: int):
    """
    1. اطلاعات فیلم و credits را از TMDb می‌گیرد
    2. با save_movie_with_cast_and_crew در دیتابیس ذخیره می‌کند
    """
    # 1. فراخوانی اطلاعات پایه فیلم
    movie_api = tmdb.Movies(tmdb_id)
    info = movie_api.info()
    release_date_str = info.get("release_date")
    release_date = datetime.strptime(release_date_str, "%Y-%m-%d").date()
    movie_data = {
        "tmdb_id": tmdb_id,
        "title": info.get("title"),
        "overview": info.get("overview"),
        "release_date": release_date,
        "popularity": info.get("popularity"),
        "vote_average": info.get("vote_average"),
        "genres": [g["name"] for g in info.get("genres", [])],
        "poster_url": None if not info.get("poster_path") else
                      f"https://image.tmdb.org/t/p/original{info['poster_path']}"
    }

    # 2. فراخوانی credits برای cast و crew
    credits = movie_api.credits()
    cast_list = credits.get("cast", [])
    crew_list = credits.get("crew", [])

    # 3. ذخیره در دیتابیس
    movie = await save_movie_with_cast_and_crew(session, movie_data, cast_list, crew_list)
    return movie