# =========================
# CONFIG

# Configuration for BGG API
# =========================

import os
from dotenv import load_dotenv

load_dotenv()

BGG_TOKEN = os.getenv("TOKEN")
BASE_URL = os.getenv("BASE_URL")

HEADERS = {
    "Authorization": f"Bearer {BGG_TOKEN}"
}
