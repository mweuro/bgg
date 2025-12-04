# =========================
# DB SCHEMA
# =========================

from sqlalchemy import text, create_engine
from logger import get_logger


logger = get_logger("db-schema")


def create_games_table(engine: create_engine) -> None:
    """
    Create the BGG games table if it does not exist.
    
    Args:
        engine: SQLAlchemy engine instance.
    
    Returns:
        None
    """
    sql = """
    CREATE TABLE IF NOT EXISTS public.bgg (
        id SERIAL PRIMARY KEY,
        bgg_id INTEGER UNIQUE NOT NULL,

        title VARCHAR(255) NOT NULL,
        release_year INTEGER,
        url TEXT NOT NULL UNIQUE,

        min_players INTEGER,
        max_players INTEGER,
        min_play_time INTEGER,
        max_play_time INTEGER,
        min_age INTEGER,

        avg_rating FLOAT,
        no_of_ratings INTEGER,
        std_deviation FLOAT,
        weight FLOAT,
        overall_rank INTEGER,
        own_count INTEGER,

        categories TEXT,
        mechanics TEXT,

        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

    logger.info("Table ensured")
