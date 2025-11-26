import os
import re
import time
from typing import List, Dict, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError


class DatabaseManager:
    """
    Manages PostgreSQL database connections and operations for BGG game data.
    """

    def __init__(self):
        self.connection_string = (
            f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
        self.engine = None
        self._connect_with_retry()

    def _connect_with_retry(self, retries=10, delay=3):
        for i in range(retries):
            try:
                self.engine = create_engine(self.connection_string)
                with self.engine.connect() as conn:
                    pass
                print("Connected to DB")
                return
            except OperationalError:
                print(f"DB not ready, retrying in {delay}s ({i+1}/{retries})...")
                time.sleep(delay)
        raise Exception("Cannot connect to database after several retries")

    def create_flattened_games_table(self):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS public.bgg (
            id SERIAL PRIMARY KEY,
            bgg_id INTEGER UNIQUE,
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
            weight VARCHAR(50),
            comments INTEGER,
            fans INTEGER,
            page_views INTEGER,
            overall_rank INTEGER,
            strategy_rank INTEGER,
            all_time_plays INTEGER,
            this_month_plays INTEGER,
            own_count INTEGER,
            prev_owned_count INTEGER,
            for_trade_count INTEGER,
            want_in_trade_count INTEGER,
            wishlist_count INTEGER,
            has_parts_count INTEGER,
            want_parts_count INTEGER,
            types TEXT,
            categories TEXT,
            mechanics TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(text(create_table_sql))
            print("Table ensured.")
        except Exception as e:
            print(f"Error creating table: {e}")

    def _convert_list_to_string(self, data_list: Optional[List]) -> str:
        if not data_list:
            return ""
        return ", ".join(str(x).strip() for x in data_list if x)

    def _extract_bgg_id(self, url: str) -> Optional[int]:
        match = re.search(r'/boardgame/(\d+)/', url)
        return int(match.group(1)) if match else None

    def save_flattened_game_data(self, game_data: Dict, retries=3, delay=2) -> bool:
        for attempt in range(retries):
            try:
                bgg_id = self._extract_bgg_id(game_data["url"])
                if not bgg_id:
                    print("ERROR: Could not extract BGG ID")
                    return False

                flat = {
                    'bgg_id': bgg_id,
                    'title': game_data.get('title_and_year.title', 'Unknown'),
                    'release_year': game_data.get('title_and_year.year'),
                    'url': game_data['url'],
                    'min_players': game_data.get('basic_info.min_players'),
                    'max_players': game_data.get('basic_info.max_players'),
                    'min_play_time': game_data.get('basic_info.min_play_time'),
                    'max_play_time': game_data.get('basic_info.max_play_time'),
                    'min_age': game_data.get('basic_info.min_age'),
                    'avg_rating': game_data.get('stats.avg_rating'),
                    'no_of_ratings': game_data.get('stats.no_of_ratings'),
                    'std_deviation': game_data.get('stats.std_deviation'),
                    'weight': game_data.get('stats.weight'),
                    'comments': game_data.get('stats.comments'),
                    'fans': game_data.get('stats.fans'),
                    'page_views': game_data.get('stats.page_views'),
                    'overall_rank': game_data.get('stats.overall_rank'),
                    'strategy_rank': game_data.get('stats.strategy_rank'),
                    'all_time_plays': game_data.get('stats.all_time_plays'),
                    'this_month_plays': game_data.get('stats.this_month'),
                    'own_count': game_data.get('stats.own'),
                    'prev_owned_count': game_data.get('stats.prev_owned'),
                    'for_trade_count': game_data.get('stats.for_trade'),
                    'want_in_trade_count': game_data.get('stats.want_in_trade'),
                    'wishlist_count': game_data.get('stats.wishlist'),
                    'has_parts_count': game_data.get('stats.has_parts'),
                    'want_parts_count': game_data.get('stats.want_parts'),
                    'types': self._convert_list_to_string(game_data.get('types', [])),
                    'categories': self._convert_list_to_string(game_data.get('categories_mechanics.categories', [])),
                    'mechanics': self._convert_list_to_string(game_data.get('categories_mechanics.mechanics', [])),
                }

                sql = text("""
                INSERT INTO public.bgg (
                    bgg_id, title, release_year, url, min_players, max_players,
                    min_play_time, max_play_time, min_age, avg_rating, no_of_ratings,
                    std_deviation, weight, comments, fans, page_views, overall_rank,
                    strategy_rank, all_time_plays, this_month_plays, own_count,
                    prev_owned_count, for_trade_count, want_in_trade_count,
                    wishlist_count, has_parts_count, want_parts_count, types,
                    categories, mechanics
                )
                VALUES (
                    :bgg_id, :title, :release_year, :url, :min_players, :max_players,
                    :min_play_time, :max_play_time, :min_age, :avg_rating,
                    :no_of_ratings, :std_deviation, :weight, :comments, :fans,
                    :page_views, :overall_rank, :strategy_rank, :all_time_plays,
                    :this_month_plays, :own_count, :prev_owned_count,
                    :for_trade_count, :want_in_trade_count, :wishlist_count,
                    :has_parts_count, :want_parts_count, :types, :categories,
                    :mechanics
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
                    comments = EXCLUDED.comments,
                    fans = EXCLUDED.fans,
                    page_views = EXCLUDED.page_views,
                    overall_rank = EXCLUDED.overall_rank,
                    strategy_rank = EXCLUDED.strategy_rank,
                    all_time_plays = EXCLUDED.all_time_plays,
                    this_month_plays = EXCLUDED.this_month_plays,
                    own_count = EXCLUDED.own_count,
                    prev_owned_count = EXCLUDED.prev_owned_count,
                    for_trade_count = EXCLUDED.for_trade_count,
                    want_in_trade_count = EXCLUDED.want_in_trade_count,
                    wishlist_count = EXCLUDED.wishlist_count,
                    has_parts_count = EXCLUDED.has_parts_count,
                    want_parts_count = EXCLUDED.want_parts_count,
                    types = EXCLUDED.types,
                    categories = EXCLUDED.categories,
                    mechanics = EXCLUDED.mechanics,
                    updated_at = CURRENT_TIMESTAMP;
                """)

                with self.engine.begin() as conn:
                    conn.execute(sql, flat)

                print(f"Saved / updated: {flat['title']}")
                return True

            except Exception as e:
                print(f"Attempt {attempt+1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    return False
