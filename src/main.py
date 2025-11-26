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
    """
    
    # Check if running in Docker
    if os.path.exists('/.dockerenv'):
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        logging.info("Docker logging configured - output to stdout")
    else:
        # Local - log to file
        os.makedirs('logs', exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"logs/bgg_scraper_{timestamp}.log"
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
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
        logging.info(f"Local logging configured. Log file: {log_filename}")




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




def main(n: int = 50):  # Zaktualizowana domyślna wartość
    """
    Main function to extract top N games from BGG and store them in PostgreSQL database.

    Args:
        n (int): Number of top games to extract. Default is 50.
    
    Returns:
        None. Just performs the extraction and storage.
    """
    logger = logging.getLogger(__name__)
    
    logger.info(f"Starting BGG scraper for top {n} games")
    
    # Connect to DB with retry logic
    logger.info("Connecting to database...")
    db = DatabaseManager()
    
    # Create table
    logger.info("Creating table...")
    db.create_flattened_games_table()

    # Get top games
    logger.info("Fetching top games...")
    top_games = BGGTopGamesExtractor.get_urls(num_games=n)
    bgg = BGGDataExtractor()

    success_count = 0
    failure_count = 0

    try:
        for i, (name, url) in enumerate(top_games.items()):
            try:
                logger.info(f"Processing {i+1}/{len(top_games)}: {name}")
                game_data = bgg.get_complete_game_data(url)
                success = db.save_flattened_game_data(game_data)
                if success:
                    logger.info(f"Successfully processed: {name}")
                    success_count += 1
                else:
                    logger.error(f"Failed to save: {name}")
                    failure_count += 1
                
                # Rate limiting between games
                if i < len(top_games) - 1:
                    logger.debug("Rate limiting - waiting 2 seconds")
                    time.sleep(2)
                    
            except Exception as e:
                logger.error(f"Error processing {name}: {e}")
                failure_count += 1
                continue
                
        logger.info(f"Completed! Successfully processed {success_count}/{len(top_games)} games. Failures: {failure_count}")
        
    except Exception as e:
        logger.critical(f"Critical error in main execution: {e}")
        raise
        
    finally:
        bgg.close()
        logger.info("BGG data extractor closed")




if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()
    
    # Setup logging based on debug flag
    log_level = logging.DEBUG if args.debug else logging.INFO
    setup_logging(level=log_level)
    
    logger = logging.getLogger(__name__)
    
    try:
        # Validate input
        if args.number <= 0:
            logger.error("Number of games must be positive")
            sys.exit(1)
            
        if args.number > 1000:
            logger.warning(f"Extracting {args.number} games might take a long time")
            
        logger.info(f"Starting extraction of {args.number} games")
        main(n=args.number)
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Application failed: {e}")
        sys.exit(1)