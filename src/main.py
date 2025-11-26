import asyncio
from data_ingestion.bgg_scraper import BGGDataExtractor, BGGTopGamesExtractor
from datetime import datetime
from postgre.postgre import DatabaseManager
import logging
import os
import sys
import time
import argparse




def setup_logging(level: int = logging.INFO):
    """
    Docker-friendly logging configuration.

    Args:
        level (int): Logging level. Default is logging.INFO.
    
    Returns:
        None. Just configures logging.
    """
    if os.path.exists('/.dockerenv'):
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
    else:
        os.makedirs('logs', exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"logs/bgg_scraper_{timestamp}.log"
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler = logging.FileHandler(log_filename, encoding='utf-8')
        file_handler.setFormatter(formatter)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger = logging.getLogger()
        root_logger.setLevel(level)
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)




def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Extract top board games from BoardGameGeek and store in PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        Examples:
            python src/main.py              # Extract top 50 games (default)
            python src/main.py -n 20        # Extract top 20 games
            python src/main.py --number 100 # Extract top 100 games
        """
    )
    
    parser.add_argument(
        '-n', '--number',
        type=int,
        default=50,
        help='Number of top games to extract (default: 50)',
        metavar='N'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    return parser.parse_args()





async def fetch_and_save(semaphore: asyncio.Semaphore, 
                         extractor: BGGDataExtractor, 
                         db: DatabaseManager, 
                         name: str, 
                         url: str) -> tuple[str, bool]:
    """
    Fetch game data and save to database with semaphore control.

    Args:
        semaphore: asyncio.Semaphore for concurrency control.
        extractor: BGGDataExtractor instance.
        db: DatabaseManager instance.
        name: Name of the game.
        url: URL of the game.
    
    Returns:
        Tuple[str, bool]: Name and success status.
    """
    async with semaphore:
        try:
            game_data = await extractor.get_complete_game_data(url)
            loop = asyncio.get_event_loop()
            success = await loop.run_in_executor(None, db.save_flattened_game_data, game_data)
            return name, success
        except Exception as e:
            return name, False



async def main(n: int = 50):
    """
    Main function to extract top N games from BGG and store them in PostgreSQL database.

    Args:
        n (int): Number of top games to extract. Default is 50.
    
    Returns:
        None. Just performs the extraction and storage.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Starting async BGG scraper for top {n} games")

    db = DatabaseManager()
    db.create_flattened_games_table()

    logger.info("Fetching top games URLs...")
    top_games = BGGTopGamesExtractor.get_urls(num_games=n)

    extractor = await BGGDataExtractor.create()
    semaphore = asyncio.Semaphore(5)  # max 5 parallel fetches
    tasks = []

    for name, url in top_games.items():
        tasks.append(fetch_and_save(semaphore, extractor, db, name, url))

    results = await asyncio.gather(*tasks)

    success_count = sum(1 for _, ok in results if ok)
    failure_count = sum(1 for _, ok in results if not ok)

    logger.info(f"Completed! Successfully processed {success_count}/{len(top_games)} games. Failures: {failure_count}")
    await extractor.close()





if __name__ == "__main__":
    args = parse_arguments()
    log_level = logging.DEBUG if args.debug else logging.INFO
    setup_logging(level=log_level)

    if args.number <= 0:
        logging.error("Number of games must be positive")
        sys.exit(1)

    try:
        asyncio.run(main(n=args.number))
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logging.critical(f"Application failed: {e}")
        sys.exit(1)