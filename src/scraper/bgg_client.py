# =================
# BGG CLIENT

# Scraper client to fetch game data from BoardGameGeek (BGG) API
# =================

import asyncio
import aiohttp
import xml.etree.ElementTree as ET

from config import BASE_URL, HEADERS
from models import Game
from logger import get_logger


# =================
# SETUP
# =================

logger = get_logger("bgg-client")
TIMEOUT = aiohttp.ClientTimeout(total=15)

# =================
# MAIN FUNCTION
# =================

async def fetch_game_data(game_id: int) -> Game | None:
    """
    Fetch game data from BGG API by game ID.

    Args:
        game_id (int): The BGG game ID.
    
    Returns:
        Game | None: The Game object if successful, None otherwise.
    """

    logger.info(f"Download data for game ID={game_id}")

    params = {
        "id": game_id,
        "stats": 1,
        "type": "boardgame"
    }

    async with aiohttp.ClientSession(headers=HEADERS, timeout=TIMEOUT) as session:
        try:
            async with session.get(f"{BASE_URL}/thing", params=params) as response:
                if response.status != 200:
                    logger.error(f"Error HTTP {response.status} for ID={game_id}")
                    return None

                text = await response.text()

        except asyncio.TimeoutError:
            logger.error(f"Timeout for ID={game_id}")
            return None

        except aiohttp.ClientError as e:
            logger.exception(f"Connection error: {e}")
            return None

    try:
        root = ET.fromstring(text)

        data = {
            "title": root.find(".//item/name[@type='primary']").attrib["value"],
            "year": int(root.find(".//item/yearpublished").attrib["value"]),
            "url": root.find(".//item/thumbnail").text,
            "min_players": int(root.find(".//item/minplayers").attrib["value"]),
            "max_players": int(root.find(".//item/maxplayers").attrib["value"]),
            "min_playtime": int(root.find(".//item/minplaytime").attrib["value"]),
            "max_playtime": int(root.find(".//item/maxplaytime").attrib["value"]),
            "age": int(root.find(".//item/minage").attrib["value"]),
            "num_ratings": int(root.find(".//item/statistics/ratings/usersrated").attrib["value"]),
            "avg_rating": float(root.find(".//item/statistics/ratings/average").attrib["value"]),
            "std_rating": float(root.find(".//item/statistics/ratings/stddev").attrib["value"]),
            "weight": float(root.find(".//item/statistics/ratings/averageweight").attrib["value"]),
            "rank": int(root.find(".//item/statistics/ratings/ranks/rank[@name='boardgame']").attrib["value"]),
            "num_owners": int(root.find(".//item/statistics/ratings/owned").attrib["value"]),
            "categories": [
                tag.attrib["value"]
                for tag in root.findall(".//item/link[@type='boardgamecategory']")
            ],
            "mechanics": [
                tag.attrib["value"]
                for tag in root.findall(".//item/link[@type='boardgamemechanic']")
            ],
        }

        game = Game(**data)
        logger.info(f"Successfully downloaded data: {game.title}")
        return game

    except Exception as e:
        logger.exception(f"Error parsing ID={game_id}: {e}")
        return None
