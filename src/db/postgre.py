# =========================
# POSTGRE SQL MANAGER
# =========================

import os
import time
from typing import Dict, Optional, List

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from src.logger import get_logger
from src.models import Game


# =========================
# LOGGER + ENV LOAD
# =========================
logger = get_logger("database")
load_dotenv()

# =========================
# DATABASE MANAGER
# =========================

class DatabaseManager:
    """
    PostgreSQL manager for BGG game data – compatible with current async scraper.
    """

    def __init__(self):
        """
        Initialize the DatabaseManager with connection string and engine.
        """
        self.connection_string = (
            f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
            f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
        )
        self.engine = None
        self._connect_with_retry()

    # =========================
    # CONNECTION
    # =========================

    def _connect_with_retry(self, retries: int = 10, delay: int = 3) -> None:
        """
        Create a SQLAlchemy engine with retry logic for PostgreSQL connection.
        
        Args:
            retries (int): Number of connection retries.
            delay (int): Delay between retries in seconds.

        Raises:
            RuntimeError: If unable to connect after retries.
        """
        for i in range(retries):
            try:
                self.engine = create_engine(self.connection_string, pool_pre_ping=True)
                with self.engine.connect():
                    pass
                logger.info("Connected to DB")
                return
            except OperationalError:
                logger.warning(f"DB not ready, retrying in {delay}s ({i+1}/{retries})...")
                time.sleep(delay)

        raise RuntimeError("Cannot connect to database after several retries")

    # =========================
    # TABLE
    # =========================

    def create_games_table(self) -> None:
        """
        Create the BGG games table if it does not exist.
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

        try:
            with self.engine.begin() as conn:
                conn.execute(text(sql))
            logger.info("Table ensured.")
        except Exception as e:
            logger.exception(f"Error creating table: {e}")

    # =========================
    # UTILS
    # =========================

    def _convert_list_to_string(self, data_list: Optional[List[str]]) -> str:
        """
        Convert a list of strings to a single comma-separated string.
        
        Args:
            data_list (Optional[List[str]]): List of strings to convert.

        Returns:
            str: Comma-separated string.
        """
        if not data_list:
            return ""
        return ", ".join(x.strip() for x in data_list if x)

    # =========================
    # SAVE GAME
    # =========================

    def save_game(self, bgg_id: int, game: Game, retries: int = 3, delay: int = 2) -> bool:
        """
        Saves or updates game based on current scraper Game model.

        Args:
            bgg_id (int): The BGG game ID.
            game (Game): The Game object to save.
            retries (int): Number of retries on failure.
            delay (int): Delay between retries in seconds.

        Returns:
            bool: True if saved/updated successfully, False otherwise.
        """

        for attempt in range(retries):
            try:
                flat: Dict = {
                    "bgg_id": bgg_id,
                    "title": game.title,
                    "release_year": game.year,
                    "url": game.url,

                    "min_players": game.min_players,
                    "max_players": game.max_players,
                    "min_play_time": game.min_playtime,
                    "max_play_time": game.max_playtime,
                    "min_age": game.age,

                    "avg_rating": game.avg_rating,
                    "no_of_ratings": game.num_ratings,
                    "std_deviation": game.std_rating,
                    "weight": game.weight,
                    "overall_rank": game.rank,
                    "own_count": game.num_owners,

                    "categories": self._convert_list_to_string(game.categories),
                    "mechanics": self._convert_list_to_string(game.mechanics),
                }

                sql = text("""
                INSERT INTO public.bgg (
                    bgg_id, title, release_year, url,
                    min_players, max_players,
                    min_play_time, max_play_time, min_age,
                    avg_rating, no_of_ratings, std_deviation,
                    weight, overall_rank, own_count,
                    categories, mechanics
                )
                VALUES (
                    :bgg_id, :title, :release_year, :url,
                    :min_players, :max_players,
                    :min_play_time, :max_play_time, :min_age,
                    :avg_rating, :no_of_ratings, :std_deviation,
                    :weight, :overall_rank, :own_count,
                    :categories, :mechanics
                )
                ON CONFLICT (bgg_id)
                DO UPDATE SET
                    title = EXCLUDED.title,
                    release_year = EXCLUDED.release_year,
                    min_players = EXCLUDED.min_players,
                    max_players = EXCLUDED.max_players,
                    min_play_time = EXCLUDED.min_play_time,
                    max_play_time = EXCLUDED.max_play_time,
                    min_age = EXCLUDED.min_age,
                    avg_rating = EXCLUDED.avg_rating,
                    no_of_ratings = EXCLUDED.no_of_ratings,
                    std_deviation = EXCLUDED.std_deviation,
                    weight = EXCLUDED.weight,
                    overall_rank = EXCLUDED.overall_rank,
                    own_count = EXCLUDED.own_count,
                    categories = EXCLUDED.categories,
                    mechanics = EXCLUDED.mechanics,
                    updated_at = CURRENT_TIMESTAMP;
                """)

                with self.engine.begin() as conn:
                    conn.execute(sql, flat)

                logger.info(f"Saved / updated: {game.title}")
                return True

            except Exception as e:
                logger.error(f"Attempt {attempt+1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    return False
