# =========================
# DB ENGINE
# =========================


import os
import time
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from src.logger import get_logger

# =========================
# LOGGER
# =========================

logger = get_logger("db-engine")

# =========================
# ENGINE CREATION
# =========================

def create_db_engine(retries: int = 10, delay: int = 3) -> create_engine:
    """
    Create a SQLAlchemy engine with retry logic for PostgreSQL connection.

    Args:
        retries (int): Number of connection retries.
        delay (int): Delay between retries in seconds.
    
    Returns:
        sqlalchemy.Engine: SQLAlchemy engine instance.
    
    Raises:
        RuntimeError: If unable to connect after retries.
    """
    connection_string = (
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )

    for i in range(retries):
        try:
            engine = create_engine(connection_string, pool_pre_ping=True)
            with engine.connect():
                pass
            logger.info("Connected to DB")
            return engine
        except OperationalError:
            logger.warning(f"DB not ready, retrying in {delay}s ({i+1}/{retries})...")
            time.sleep(delay)

    raise RuntimeError("Cannot connect to database")
