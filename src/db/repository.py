# =========================
# DB REPOSITORY
# =========================

from typing import List, Dict
from sqlalchemy import text

from src.logger import get_logger
from src.models import Game

# =========================
# LOGGER
# =========================

logger = get_logger("db-repository")

# =========================
# HELPERS
# =========================

def _list_to_str(values: List[str]) -> str:
    """
    Convert a list of strings to a single comma-separated string.
    
    Args:
        values (List[str]): List of strings to convert.

    Returns:
        str: Comma-separated string.
    """
    return ", ".join(v.strip() for v in values if v)

# =========================
# BATCH SAVE GAMES
# =========================

def batch_save_games(
    engine,
    games: List[tuple[int, Game]],
    batch_size: int = 100
) -> None:
    """
    Batch UPSERT games to PostgreSQL.

    Args:
        engine: SQLAlchemy engine instance.
        games (List[tuple[int, Game]]): List of tuples containing BGG ID and Game object.
        batch_size (int): Number of records to insert per batch.

    Returns:
        None
    """
    if not games:
        return

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

    records: List[Dict] = []

    for bgg_id, game in games:
        records.append({
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

            "categories": _list_to_str(game.categories),
            "mechanics": _list_to_str(game.mechanics),
        })

    with engine.begin() as conn:
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            conn.execute(sql, batch)

    logger.info(f"Batch saved: {len(records)} games")
