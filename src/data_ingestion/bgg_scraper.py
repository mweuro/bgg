import asyncio
from bs4 import BeautifulSoup
import json
import logging
from playwright.async_api import async_playwright, Playwright, Browser, BrowserContext, Page, TimeoutError as PlaywrightTimeoutError
import re
import requests
from typing import Dict, List, Optional, Tuple, Union




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
    def save_urls_to_file(cls, 
                          num_games: int = 100, 
                          filename: str = "best_games_urls.json") -> None:
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
    Asynchronous BoardGameGeek data extractor using Playwright (async).
    """

    def __init__(self, 
                 playwright: Playwright, 
                 browser: Browser, context: 
                 BrowserContext, 
                 timeout: int = 30000):
        """
        Initialize the extractor with Playwright browser and context.

        Args:
            playwright (Playwright): Playwright instance.
            browser (Browser): Playwright browser instance.
            context (BrowserContext): Playwright browser context.
            timeout (int): Timeout for page operations in milliseconds.
        
        Returns:
            None. Just initializes the instance.
        """
        self._playwright = playwright
        self.browser = browser
        self.context = context
        self.timeout = timeout

    @classmethod
    async def create(cls, 
                     headless: bool = True, 
                     timeout: int = 30000, 
                     browser_args: Optional[List[str]] = None) -> "BGGDataExtractor":
        """
        Async factory to create extractor (because __init__ cannot be async).

        Args:
            headless (bool): Whether to run browser in headless mode.
            timeout (int): Timeout for page operations in milliseconds.
            browser_args (Optional[List[str]]): Additional browser launch arguments.
        
        Returns:
            BGGDataExtractorAsync: Initialized extractor instance.
        """
        if browser_args is None:
            browser_args = [
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-renderer-backgrounding",
                "--disable-background-timer-throttling",
            ]

        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(headless=headless, args=browser_args)
        context = await browser.new_context(viewport={"width": 1200, "height": 1000})
        logger.info("Playwright browser/context created (async)")
        return cls(playwright, browser, context, timeout=timeout)

    @staticmethod
    def _normalize_key(key: str) -> str:
        """
        Convert key to snake_case.
        
        Args:
            key (str): Original key.
        
        Returns:
            str: Normalized key.
        """
        key = key.lower().strip()
        key = re.sub(r"[^a-z0-9]+", "_", key)
        return key.strip("_")

    @staticmethod
    def _extract_numeric_value(raw_value: str) -> Union[str, int, float]:
        """
        Convert string value to appropriate numeric type if possible.

        Args:
            raw_value (str): Raw string value.
        
        Returns:
            Union[str, int, float]: Converted numeric value or original string.
        """
        if raw_value is None:
            return raw_value
        cleaned_value = raw_value.strip().replace(",", "")
        if re.match(r"^-?\d+\.\d+$", cleaned_value):
            try:
                return float(cleaned_value)
            except ValueError:
                return raw_value
        elif cleaned_value.isdigit() or (cleaned_value.startswith('-') and cleaned_value[1:].isdigit()):
            try:
                return int(cleaned_value)
            except ValueError:
                return raw_value
        return raw_value

    async def _safe_query_selector(self, page: Page, selector: str) -> Optional[str]:
        """
        Safely get text content from selector with error handling (async).

        Args:
            page (Page): Playwright page instance.
            selector (str): CSS selector.
        
        Returns:
            Optional[str]: Text content or None if not found/error.
        """
        try:
            element = await page.query_selector(selector)
            if element:
                text = await element.text_content()
                return text.strip() if text else None
            return None
        except Exception as e:
            logger.debug(f"Error querying selector {selector}: {e}")
            return None

    async def _safe_query_selector_all(self, 
                                       page: Page, 
                                       selector: str) -> List:
        """
        Safely get all elements matching selector (async).
        Returns list of ElementHandle-like objects.

        Args:
            page (Page): Playwright page instance.
            selector (str): CSS selector.
        
        Returns:
            List: List of matching elements or empty list if error.
        """
        try:
            return await page.query_selector_all(selector)
        except Exception as e:
            logger.debug(f"Error querying all selectors {selector}: {e}")
            return []

    @staticmethod
    def _extract_range_values(text: str, pattern: str) -> Optional[Tuple[int, int]]:
        """
        Extract min and max values from range text.

        Args:
            text (str): Text containing range.
            pattern (str): Regex pattern with two capture groups for min and max.
        
        Returns:
            Optional[Tuple[int, int]]: Tuple of (min, max) or None if
        """
        if not text:
            return None
        match = re.search(pattern, text)
        if match:
            return int(match.group(1)), int(match.group(2))
        return None

    @staticmethod
    def _extract_single_value(text: str, pattern: str) -> Optional[int]:
        """
        Extract single numeric value from text.

        Args:
            text (str): Text containing the value.
            pattern (str): Regex pattern with one capture group.
        
        Returns:
            Optional[int]: Extracted integer value or None if not found.
        """
        if not text:
            return None
        match = re.search(pattern, text)
        return int(match.group(1)) if match else None

    @staticmethod
    def _is_more_link(text: str) -> bool:
        """
        Check if text is a 'more' link.

        Args:
            text (str): Text to check.
        
        Returns:
            bool: True if it's a 'more' link, False otherwise.
        """
        if not text:
            return False
        text_lower = text.lower()
        return text.startswith('+') or 'more' in text_lower

    async def get_game_stats(self, url: str) -> Dict[str, Union[str, int, float]]:
        """
        Extract game statistics from the stats page.

        Args:
            url (str): Base URL of the game.
        
        Returns:
            Dict[str, Union[str, int, float]]: Dictionary of game statistics.
        """
        stats_url = f"{url.rstrip('/')}/stats"
        logger.info(f"Fetching stats from: {stats_url}")

        try:
            page = await self.context.new_page()
            try:
                await page.goto(stats_url, wait_until="networkidle", timeout=self.timeout)
            except PlaywrightTimeoutError:
                # fallback to domcontentloaded if networkidle times out
                logger.debug("Timeout on networkidle, retrying domcontentloaded")
                await page.goto(stats_url, wait_until="domcontentloaded", timeout=self.timeout)

            # Wait for stats list if present
            try:
                await page.wait_for_selector("li.outline-item", state="attached", timeout=int(self.timeout/2))
            except PlaywrightTimeoutError:
                # nothing found — return empty
                logger.debug("No 'li.outline-item' found on stats page")
                await page.close()
                return {}

            stat_items = await page.query_selector_all("li.outline-item")
            stats: Dict[str, Union[str, int, float]] = {}

            for item in stat_items:
                try:
                    text_content = (await item.inner_text()) or ""
                    text_content = text_content.strip()
                    parts = text_content.split("\n")
                    if len(parts) >= 2:
                        key = self._normalize_key(parts[0])
                        value = self._extract_numeric_value(parts[1])
                        stats[key] = value
                except Exception as e:
                    logger.warning(f"Error processing stat item: {e}")
                    continue

            await page.close()
            logger.debug(f"Extracted {len(stats)} stats for {url}")
            return stats

        except Exception as e:
            logger.error(f"Error fetching stats from {url}: {e}")
            return {}

    async def get_game_categories_and_mechanics(self, url: str) -> Dict[str, List[str]]:
        """
        Extract game categories and mechanics from the credits page.

        Args:
            url (str): Base URL of the game.

        Returns:
            Dict[str, List[str]]: Dictionary with categories and mechanics lists.
        """
        credits_url = f"{url.rstrip('/')}/credits"
        logger.info(f"Fetching categories/mechanics from: {credits_url}")

        try:
            page = await self.context.new_page()
            try:
                await page.goto(credits_url, wait_until="networkidle", timeout=self.timeout)
            except PlaywrightTimeoutError:
                await page.goto(credits_url, wait_until="domcontentloaded", timeout=self.timeout)

            result = {"categories": [], "mechanics": []}

            # categories
            category_element = await page.query_selector('li:has(a[name="boardgamecategory"])')
            if category_element:
                category_links = await category_element.query_selector_all('div.ng-scope a.ng-binding')
                cats = []
                for link in category_links:
                    txt = (await link.inner_text()) or ""
                    txt = txt.strip()
                    if not self._is_more_link(txt):
                        cats.append(txt)
                result["categories"] = cats

            # mechanics
            mechanic_element = await page.query_selector('li:has(a[name="boardgamemechanic"])')
            if mechanic_element:
                mechanic_links = await mechanic_element.query_selector_all('div.ng-scope a.ng-binding')
                mechs = []
                for link in mechanic_links:
                    txt = (await link.inner_text()) or ""
                    txt = txt.strip()
                    if not self._is_more_link(txt):
                        mechs.append(txt)
                result["mechanics"] = mechs

            await page.close()
            logger.debug(f"Extracted {len(result['categories'])} categories and {len(result['mechanics'])} mechanics for {url}")
            return result

        except Exception as e:
            logger.error(f"Error fetching categories/mechanics from {url}: {e}")
            return {"categories": [], "mechanics": []}

    async def get_game_types(self, url: str) -> List[str]:
        """
        Extract game types from the main game page.

        Args:
            url (str): Base URL of the game.

        Returns:
            List[str]: List of game types.
        """
        logger.info(f"Fetching game types from: {url}")
        try:
            page = await self.context.new_page()
            try:
                await page.goto(url, wait_until="networkidle", timeout=self.timeout)
            except PlaywrightTimeoutError:
                await page.goto(url, wait_until="domcontentloaded", timeout=self.timeout)

            try:
                await page.wait_for_selector("div.game-classification", state="attached", timeout=int(self.timeout/2))
            except PlaywrightTimeoutError:
                logger.debug("No 'div.game-classification' found")
                await page.close()
                return []

            type_elements = await page.query_selector_all("div.game-classification a.ng-binding:not([title])")
            types = []
            for element in type_elements:
                txt = (await element.inner_text()) or ""
                txt = txt.strip()
                if not self._is_more_link(txt):
                    types.append(txt)

            await page.close()
            logger.debug(f"Extracted {len(types)} game types for {url}")
            return types

        except Exception as e:
            logger.error(f"Error fetching game types from {url}: {e}")
            return []

    async def get_title_and_year(self, url: str) -> Dict[str, Optional[str]]:
        """
        Extract game title and release year from the main game page.

        Args:
            url (str): Base URL of the game.

        Returns:
            Dict[str, Optional[str]]: Dictionary with title and year.
        """
        logger.info(f"Fetching title and year from: {url}")
        try:
            page = await self.context.new_page()
            try:
                await page.goto(url, wait_until="networkidle", timeout=self.timeout)
            except PlaywrightTimeoutError:
                await page.goto(url, wait_until="domcontentloaded", timeout=self.timeout)

            result = {"title": None, "year": None}

            try:
                await page.wait_for_selector(".game-header-title-info", state="attached", timeout=int(self.timeout/2))
            except PlaywrightTimeoutError:
                logger.debug("Title section not found")

            title_element = await page.query_selector("h1 span[itemprop='name']")
            if title_element:
                text = await title_element.inner_text()
                result["title"] = text.strip() if text else None

            year_element = await page.query_selector("span.game-year")
            if year_element:
                year_text = await year_element.inner_text() or ""
                year_text = year_text.strip()
                year_match = re.search(r'\((\d{4})\)', year_text)
                if year_match:
                    result["year"] = year_match.group(1)

            if result['title'] and result['year']:
                logger.info(f"Found: {result['title']} ({result['year']})")
            elif result['title']:
                logger.info(f"Found title: {result['title']}")
            else:
                logger.warning(f"Could not extract title and year from {url}")

            await page.close()
            return result

        except Exception as e:
            logger.error(f"Error fetching title and year from {url}: {e}")
            return {"title": None, "year": None}

    async def get_game_basic_info(self, url: str) -> Dict[str, Optional[int]]:
        """
        Extract basic game info: min/max players, play time, min age.

        Args:
            url (str): Base URL of the game.
        
        Returns:
            Dict[str, Optional[int]]: Dictionary with basic game info.
        """
        logger.info(f"Fetching basic info from: {url}")
        try:
            page = await self.context.new_page()
            try:
                await page.goto(url, wait_until="networkidle", timeout=self.timeout)
            except PlaywrightTimeoutError:
                await page.goto(url, wait_until="domcontentloaded", timeout=self.timeout)

            try:
                await page.wait_for_selector("div.game-header-gameplay", state="attached", timeout=int(self.timeout/2))
            except PlaywrightTimeoutError:
                logger.debug("No gameplay section found")

            game_info = {
                "min_players": None,
                "max_players": None,
                "min_play_time": None,
                "max_play_time": None,
                "min_age": None
            }

            players_text = await self._safe_query_selector(page, 'p.gameplay-item-primary:has-text("Players")')
            if players_text:
                players_range = self._extract_range_values(players_text, r'(\d+)\s*–\s*(\d+)')
                if players_range:
                    game_info["min_players"], game_info["max_players"] = players_range
                else:
                    single_player = self._extract_single_value(players_text, r'(\d+)\s*Players')
                    if single_player:
                        game_info["min_players"] = game_info["max_players"] = single_player

            time_text = await self._safe_query_selector(page, 'p.gameplay-item-primary:has-text("Min")')
            if time_text:
                time_range = self._extract_range_values(time_text, r'(\d+)\s*–\s*(\d+)\s*Min')
                if time_range:
                    game_info["min_play_time"], game_info["max_play_time"] = time_range
                else:
                    single_time = self._extract_single_value(time_text, r'(\d+)\s*Min')
                    if single_time:
                        game_info["min_play_time"] = game_info["max_play_time"] = single_time

            age_text = await self._safe_query_selector(page, 'p.gameplay-item-primary:has-text("Age:")')
            if age_text:
                game_info["min_age"] = self._extract_single_value(age_text, r'Age:\s*(\d+)\+')

            await page.close()
            logger.debug(f"Extracted basic info: {game_info}")
            return game_info

        except Exception as e:
            logger.error(f"Error fetching basic info from {url}: {e}")
            # return a dict with keys present (preserves behaviour)
            return {
                "min_players": None,
                "max_players": None,
                "min_play_time": None,
                "max_play_time": None,
                "min_age": None
            }

    async def get_complete_game_data(self, url: str) -> Dict:
        """
        Extract all available game data in one call.

        Args:
            url (str): Base URL of the game.
        
        Returns:
            Dict: Flattened dictionary with all game data.
        """
        logger.info(f"Fetching complete data for: {url}")

        async def flatten_dict(d, parent_key: str = "", sep: str = "."):
            items = {}
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.update(await flatten_dict(v, new_key, sep=sep))
                else:
                    items[new_key] = v
            return items

        # small rate limit to avoid bursty behavior (async-friendly)
        await asyncio.sleep(0.05)

        # run subtasks concurrently per-game (they share the same browser context)
        title_year_task = asyncio.create_task(self.get_title_and_year(url))
        basic_info_task = asyncio.create_task(self.get_game_basic_info(url))
        stats_task = asyncio.create_task(self.get_game_stats(url))
        types_task = asyncio.create_task(self.get_game_types(url))
        cat_mech_task = asyncio.create_task(self.get_game_categories_and_mechanics(url))

        results = await asyncio.gather(
            title_year_task, basic_info_task, stats_task, types_task, cat_mech_task,
            return_exceptions=True
        )

        # handle exceptions gracefully, preserving original semantics
        def normalize_res(res: Union[Dict, List, Exception]) -> Union[Dict, List]:
            """
            Normalize result, converting exceptions to empty structures.

            Args:
                res: Result from subtask.
            
            Returns:
                Normalized result.
            """
            if isinstance(res, Exception):
                logger.warning(f"Subtask failed: {res}")
                # return an "empty" shaped value
                if isinstance(res, dict):
                    return res
                return {} if not isinstance(res, list) else []

            return res

        title_and_year = normalize_res(results[0])
        basic_info = normalize_res(results[1])
        stats = normalize_res(results[2])
        types = normalize_res(results[3])
        categories_mechanics = normalize_res(results[4])

        flattened = await flatten_dict({
            "title_and_year": title_and_year,
            "url": url,
            "basic_info": basic_info,
            "stats": stats,
            "types": types,
            "categories_mechanics": categories_mechanics,
        })

        logger.info(f"Successfully extracted data for {flattened.get('title_and_year.title', 'unknown')}")
        return flattened

    async def batch_extract_data(self, 
                                 urls: List[str], 
                                 concurrency: int = 10, 
                                 delay_between_starts: float = 0.0) -> Dict[str, Dict]:
        """
        Extract data for multiple games with controlled concurrency.

        Args:
            urls: list of URLs
            concurrency: max concurrent in-flight tasks
            delay_between_starts: small delay between starting tasks (throttling)
        
        Returns:
            Dict[str, Dict]: Mapping of URL to extracted data.
        """
        sem = asyncio.Semaphore(concurrency)
        results: Dict[str, Dict] = {}

        async def worker(url: str):
            """
            Worker to extract data for a single URL with semaphore control.

            Args:
                url (str): Game URL.

            Returns:
                Tuple[str, Dict]: URL and extracted data.
            """
            async with sem:
                try:
                    data = await self.get_complete_game_data(url)
                    return url, data
                except Exception as e:
                    logger.error(f"Failed to extract {url}: {e}")
                    return url, {"error": str(e)}

        tasks = []
        for u in urls:
            tasks.append(asyncio.create_task(worker(u)))
            if delay_between_starts:
                await asyncio.sleep(delay_between_starts)

        for coro in asyncio.as_completed(tasks):
            url, data = await coro
            results[url] = data

        return results

    async def close(self):
        """
        Clean up browser/context/playwright.

        Args:
            None.
        
        Returns:
            None. Just cleans up resources.
        """
        try:
            await self.context.close()
            await self.browser.close()
            await self._playwright.stop()
            logger.info("BGGDataExtractorAsync resources closed")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")