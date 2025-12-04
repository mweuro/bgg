import asyncio
import logging
import os
import random
import sys
import argparse
from typing import List

from logger import get_logger
from models import Game
from scraper.bgg_client import fetch_game_data
from db.engine import create_db_engine
from db.schema import create_games_table
from db.repository import batch_save_games


# =========================
# LOGGER
# =========================

logger = get_logger(__name__)


# =========================
# CLI ARGUMENTS
# =========================

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="BGG async scraper from TXT IDs --> PostgreSQL (batch insert)"
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit how many IDs to process"
    )

    return parser.parse_args()


# =========================
# LOAD IDS FROM TXT
# =========================

def load_game_ids_from_txt(limit: int | None = None) -> List[int]:
    path = "data/id.txt"

    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        ids = [int(line.strip()) for line in f if line.strip().isdigit()]

    return ids[:limit] if limit else ids


# =========================
# ASYNC WORKER
# =========================


async def fetch_with_semaphore(semaphore: asyncio.Semaphore, 
                               game_id: int) -> tuple[int, Game | None]:
    """
    Fetch game data with semaphore to limit concurrency.

    Args:
        semaphore (asyncio.Semaphore): Semaphore to limit concurrent requests.
        game_id (int): BGG game ID.
    
    Returns:
        tuple[int, Game | None]: Tuple of game ID and fetched Game object or None.
    """
    async with semaphore:
        await asyncio.sleep(10.0) # Otherwise http 429 Too Many Requests
        return game_id, await fetch_game_data(game_id)


# =========================
# MAIN PIPELINE
# =========================

async def main(limit: int | None) -> None:
    """
    Main async pipeline to scrape BGG game data from TXT IDs and save to PostgreSQL.

    Args:
        limit (int | None): Limit how many IDs to process.
    
    Returns:
        None
    """
    logger.info("Starting async BGG scraper from TXT IDs")

    # 1. DB
    engine = create_db_engine()
    create_games_table(engine)

    # 2. LOAD IDS
    game_ids = load_game_ids_from_txt(limit)
    logger.info(f"Loaded {len(game_ids)} game IDs")

    # 3. ASYNC SCRAPING
    semaphore = asyncio.Semaphore(5)

    tasks = [
        fetch_with_semaphore(semaphore, game_id)
        for game_id in game_ids
    ]

    results = await asyncio.gather(*tasks)

    # 4. FILTER RESULTS
    payload = [
        (game_id, game)
        for game_id, game in results
        if game is not None
    ]

    logger.info(f"Successfully fetched {len(payload)}/{len(game_ids)} games")

    # 5. BATCH INSERT
    batch_save_games(engine, payload, batch_size=100)

    logger.info("DONE")


# =========================
# ENTRYPOINT
# =========================

if __name__ == "__main__":
    args = parse_arguments()

    try:
        asyncio.run(main(limit=args.limit))
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.exception("Application failed")
        sys.exit(1)
