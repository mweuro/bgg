import logging
from bs4 import BeautifulSoup
import json
import requests
import re
from typing import Dict, List, Optional, Union
from playwright.sync_api import sync_playwright
import time




# Configure logger
logger = logging.getLogger(__name__)




class BGGTopGamesExtractor:
    """
    Class to get URLs of the best board games from BoardGameGeek.
    """

    BASE_URL = "https://boardgamegeek.com"
    RANKING_URL = "/browse/boardgame/page/{}"
    GAMES_PER_PAGE = 100

    @classmethod
    def get_urls(cls, num_games: int) -> Dict[str, str]:
        """
        Get URLs of the best board games from BoardGameGeek.

        Args:
            num_games (int, optional): Number of top games to retrieve. Defaults to 100.

        Returns:
            Dict[str, str]: Dictionary with game names as keys and URLs as values.
        """
        pages_needed = (num_games - 1) // cls.GAMES_PER_PAGE + 1
        games = {}

        for page_num in range(1, pages_needed + 1):
            page_url = cls.BASE_URL + cls.RANKING_URL.format(page_num)
            
            try:
                response = requests.get(page_url, timeout=10)
                response.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"Error fetching page {page_num}: {e}")
                continue

            soup = BeautifulSoup(response.content, "lxml")
            game_links = soup.find_all("a", class_="primary")

            for link in game_links:
                if len(games) >= num_games:
                    break
                    
                game_url = cls.BASE_URL + link.get("href")
                game_name = link.text.strip()
                games[game_name] = game_url

            if len(games) >= num_games:
                break

        logger.info(f"Retrieved {len(games)} game URLs")
        return games

    @classmethod
    def save_urls_to_file(cls, num_games: int = 100, filename: str = "best_games_urls.json") -> None:
        """
        Save game URLs to a JSON file.

        Args:
            num_games (int): Number of games to retrieve.
            filename (str): Output filename.
        
        Returns:
            None: Just saves the file.
        """
        games = cls.get_urls(num_games)
        
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(games, file, indent=4, ensure_ascii=False)
        
        logger.info(f"Saved {len(games)} game URLs to {filename}")




class BGGDataExtractor:
    """
    Extracts game data from BoardGameGeek using Playwright.
    """

    def __init__(self, headless: bool = True, timeout: int = 30000):
        """
        Initialize the game data extractor.

        Args:
            headless (bool): Whether to run browser in headless mode.
            timeout (int): Page load timeout in milliseconds.
        
        Returns:
            None: Just initializes the extractor.
        """
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=headless,
            args=['--no-sandbox', '--disable-dev-shm-usage']
        )
        self.page = self.browser.new_page()
        self.page.set_viewport_size({"width": 1200, "height": 1000})
        self.timeout = timeout
        logger.info("BGGDataExtractor initialized")

    def _normalize_key(self, key: str) -> str:
        """Convert key to snake_case."""
        key = key.lower().strip()
        key = re.sub(r"[^a-z0-9]+", "_", key)
        return key.strip("_")

    def _extract_numeric_value(self, raw_value: str) -> Union[str, int, float]:
        """
        Convert string value to appropriate numeric type if possible.
        
        Args:
            raw_value (str): Raw string value to convert.

        Returns:
            Union[str, int, float]: Converted value.
        """
        cleaned_value = raw_value.strip().replace(",", "")
        
        if re.match(r"^-?\d+\.\d+$", cleaned_value):
            return float(cleaned_value)
        elif cleaned_value.isdigit() or (cleaned_value.startswith('-') and cleaned_value[1:].isdigit()):
            return int(cleaned_value)
        
        return raw_value

    def _safe_query_selector(self, selector: str) -> Optional[str]:
        """
        Safely get text content from selector with error handling.
        
        Args:
            selector (str): CSS selector.

        Returns:
            Optional[str]: Text content or None if not found.
        """
        try:
            element = self.page.query_selector(selector)
            return element.text_content().strip() if element else None
        except Exception as e:
            logger.debug(f"Error querying selector {selector}: {e}")
            return None

    def _safe_query_selector_all(self, selector: str) -> List:
        """
        Safely get all elements matching selector.
        
        Args:
            selector (str): CSS selector.

        Returns:
            List: List of elements or empty list if none found.
        """
        try:
            return self.page.query_selector_all(selector)
        except Exception as e:
            logger.debug(f"Error querying all selectors {selector}: {e}")
            return []

    def _extract_range_values(self, text: str, pattern: str) -> Optional[tuple]:
        """
        Extract min and max values from range text.
        
        Args:
            text (str): Text containing range.
            pattern (str): Regex pattern to extract min and max.
        
        Returns:
            Optional[tuple]: Tuple of (min, max) or None if not found.
        """
        if not text:
            return None
        match = re.search(pattern, text)
        if match:
            return int(match.group(1)), int(match.group(2))
        return None

    def _extract_single_value(self, text: str, pattern: str) -> Optional[int]:
        """
        Extract single numeric value from text.

        Args:
            text (str): Text containing the value.
            pattern (str): Regex pattern to extract the value.
        
        Returns:
            Optional[int]: Extracted value or None if not found.
        """
        if not text:
            return None
        match = re.search(pattern, text)
        return int(match.group(1)) if match else None

    def get_game_stats(self, url: str) -> Dict[str, Union[str, int, float]]:
        """
        Extract game statistics from the stats page using HTML parsing.

        Args:
            url (str): Game URL.

        Returns:
            Dict[str, Union[str, int, float]]: Game statistics.
        """
        stats_url = f"{url}/stats"
        logger.info(f"Fetching stats from: {stats_url}")

        try:
            self.page.goto(stats_url, wait_until="networkidle", timeout=self.timeout)
            
            # Wait for stats to load
            self.page.wait_for_selector("li.outline-item", state="attached", timeout=self.timeout)
            
            stat_items = self.page.query_selector_all("li.outline-item")
            stats = {}
            
            for item in stat_items:
                try:
                    text_content = item.inner_text().strip()
                    parts = text_content.split("\n")
                    
                    if len(parts) >= 2:
                        key = self._normalize_key(parts[0])
                        value = self._extract_numeric_value(parts[1])
                        stats[key] = value
                except Exception as e:
                    logger.warning(f"Error processing stat item: {e}")
                    continue

            logger.debug(f"Extracted {len(stats)} stats for {url}")
            return stats
            
        except Exception as e:
            logger.error(f"Error fetching stats from {url}: {e}")
            return {}

    def get_game_categories_and_mechanics(self, url: str) -> Dict[str, List[str]]:
        """
        Extract game categories and mechanics.

        Args:
            url (str): Game URL.

        Returns:
            Dict[str, List[str]]: Categories and mechanics.
        """
        credits_url = f"{url.rstrip('/')}/credits"
        
        try:
            self.page.goto(credits_url, wait_until="networkidle", timeout=self.timeout)
            
            result = {"categories": [], "mechanics": []}
            
            # Extract categories
            category_element = self.page.query_selector('li:has(a[name="boardgamecategory"])')
            if category_element:
                category_links = category_element.query_selector_all('div.ng-scope a.ng-binding')
                result["categories"] = [
                    link.inner_text().strip() 
                    for link in category_links 
                    if not self._is_more_link(link.inner_text())
                ]

            # Extract mechanics
            mechanic_element = self.page.query_selector('li:has(a[name="boardgamemechanic"])')
            if mechanic_element:
                mechanic_links = mechanic_element.query_selector_all('div.ng-scope a.ng-binding')
                result["mechanics"] = [
                    link.inner_text().strip() 
                    for link in mechanic_links 
                    if not self._is_more_link(link.inner_text())
                ]

            logger.debug(f"Extracted {len(result['categories'])} categories and {len(result['mechanics'])} mechanics for {url}")
            return result
            
        except Exception as e:
            logger.error(f"Error fetching categories/mechanics from {url}: {e}")
            return {"categories": [], "mechanics": []}

    def _is_more_link(self, text: str) -> bool:
        """
        Check if text is a 'more' link.
        
        Args:
            text (str): String to check.
        
        Returns:
            bool: Value whether string is a `more` link or not.
        """
        if not text:
            return False
        text_lower = text.lower()
        return text.startswith('+') or 'more' in text_lower

    def get_game_types(self, url: str) -> List[str]:
        """
        Extract game types from main page.

        Args:
            url (str): Game URL.

        Returns:
            List[str]: List of game types.
        """
        try:
            self.page.goto(url, wait_until="networkidle", timeout=self.timeout)
            self.page.wait_for_selector("div.game-classification", state="attached", timeout=self.timeout)
            
            type_elements = self.page.query_selector_all("div.game-classification a.ng-binding:not([title])")
            
            types = [
                element.inner_text().strip() 
                for element in type_elements 
                if not self._is_more_link(element.inner_text())
            ]
            
            logger.debug(f"Extracted {len(types)} game types for {url}")
            return types
            
        except Exception as e:
            logger.error(f"Error fetching game types from {url}: {e}")
            return []

    def get_title_and_year(self, url: str) -> Dict[str, Optional[str]]:
        """
        Extract game title and release year from the main page.
        
        Args:
            url (str): Game URL.
            
        Returns:
            Dict: Contains 'title' and 'year' keys.
        """
        logger.info(f"Fetching title and year from: {url}")
        
        try:
            self.page.goto(url, wait_until="networkidle", timeout=self.timeout)
            
            result = {"title": None, "year": None}
            
            # Wait for the title section to load
            self.page.wait_for_selector(".game-header-title-info", state="attached", timeout=self.timeout)
            
            # Extract title
            title_element = self.page.query_selector("h1 span[itemprop='name']")
            if title_element:
                result["title"] = title_element.inner_text().strip()
            
            # Extract year
            year_element = self.page.query_selector("span.game-year")
            if year_element:
                year_text = year_element.inner_text().strip()
                year_match = re.search(r'\((\d{4})\)', year_text)
                if year_match:
                    result["year"] = year_match.group(1)
            
            if result['title'] and result['year']:
                logger.info(f"Found: {result['title']} ({result['year']})")
            elif result['title']:
                logger.info(f"Found title: {result['title']}")
            else:
                logger.warning(f"Could not extract title and year from {url}")
                
            return result
            
        except Exception as e:
            logger.error(f"Error fetching title and year from {url}: {e}")
            return {"title": None, "year": None}
    
    def get_game_basic_info(self, url: str) -> Dict[str, Optional[int]]:
        """
        Extract basic game information (players, playtime, age).

        Args:
            url (str): Game URL.

        Returns:
            Dict[str, Optional[int]]: Basic game information.
        """
        logger.info(f"Fetching basic info from: {url}")
        
        try:
            self.page.goto(url, wait_until="networkidle", timeout=self.timeout)
            self.page.wait_for_selector("div.game-header-gameplay", state="attached", timeout=self.timeout)
            
            game_info = {
                "min_players": None,
                "max_players": None,
                "min_play_time": None,
                "max_play_time": None,
                "min_age": None
            }

            # Extract players information
            players_text = self._safe_query_selector('p.gameplay-item-primary:has-text("Players")')
            if players_text:
                players_range = self._extract_range_values(players_text, r'(\d+)\s*–\s*(\d+)')
                if players_range:
                    game_info["min_players"], game_info["max_players"] = players_range
                else:
                    single_player = self._extract_single_value(players_text, r'(\d+)\s*Players')
                    if single_player:
                        game_info["min_players"] = game_info["max_players"] = single_player

            # Extract playtime information
            time_text = self._safe_query_selector('p.gameplay-item-primary:has-text("Min")')
            if time_text:
                time_range = self._extract_range_values(time_text, r'(\d+)\s*–\s*(\d+)\s*Min')
                if time_range:
                    game_info["min_play_time"], game_info["max_play_time"] = time_range
                else:
                    single_time = self._extract_single_value(time_text, r'(\d+)\s*Min')
                    if single_time:
                        game_info["min_play_time"] = game_info["max_play_time"] = single_time

            # Extract age information
            age_text = self._safe_query_selector('p.gameplay-item-primary:has-text("Age:")')
            if age_text:
                game_info["min_age"] = self._extract_single_value(age_text, r'Age:\s*(\d+)\+')

            logger.debug(f"Extracted basic info: {game_info}")
            return game_info
            
        except Exception as e:
            logger.error(f"Error fetching basic info from {url}: {e}")
            return {key: None for key in game_info}

    def get_complete_game_data(self, url: str) -> Dict:
        """
        Extract all available game data in one call.

        Args:
            url (str): Game URL.

        Returns:
            Dict: Complete game data.
        """
        logger.info(f"Fetching complete data for: {url}")

        def flatten_dict(d, parent_key: str = "", sep: str = ".") -> Dict:
            items = {}
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.update(flatten_dict(v, new_key, sep=sep))
                else:
                    items[new_key] = v
            return items

        
        # Rate limiting to avoid overwhelming the server
        time.sleep(1)

        flattened_data = flatten_dict({
            "title_and_year": self.get_title_and_year(url),
            "url": url,
            "basic_info": self.get_game_basic_info(url),
            "stats": self.get_game_stats(url),
            "types": self.get_game_types(url),
            "categories_mechanics": self.get_game_categories_and_mechanics(url), 
        })

        logger.info(f"Successfully extracted data for {flattened_data.get('title_and_year.title', 'unknown')}")
        return flattened_data

    def batch_extract_data(self, urls: List[str], delay: float = 2.0) -> Dict[str, Dict]:
        """
        Extract data for multiple games with rate limiting.

        Args:
            urls (List[str]): List of game URLs.
            delay (float): Delay between requests in seconds.

        Returns:
            Dict[str, Dict]: Dictionary with URLs as keys and game data as values.
        """
        results = {}
        
        for i, url in enumerate(urls):
            logger.info(f"Processing {i+1}/{len(urls)}: {url}")
            
            try:
                results[url] = self.get_complete_game_data(url)
            except Exception as e:
                logger.error(f"Failed to extract data from {url}: {e}")
                results[url] = {"error": str(e)}
            
            # Rate limiting
            if i < len(urls) - 1:
                time.sleep(delay)
                
        logger.info(f"Batch extraction completed: {len(results)}/{len(urls)} successful")
        return results

    def close(self) -> None:
        """
        Clean up resources.
        
        Returns:
            None: Just cleans up.
        """
        try:
            self.browser.close()
            self.playwright.stop()
            logger.info("BGGDataExtractor resources closed")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")