# =========================
# MODELS

# Define Pydantic models for data validation and serialization
# =========================


from pydantic import BaseModel
from typing import List


class Game(BaseModel):
    title: str
    year: int
    url: str
    min_players: int
    max_players: int
    min_playtime: int
    max_playtime: int
    age: int
    num_ratings: int
    avg_rating: float
    std_rating: float
    weight: float
    rank: int
    num_owners: int
    categories: List[str]
    mechanics: List[str]
    # class Config:
    #     orm_mode = True