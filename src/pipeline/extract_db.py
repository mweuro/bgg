import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()


def get_connection_string() -> str:
    return (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@localhost:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )


def export_bgg_to_parquet(output_path: str = "data/bgg.parquet"):
    """
    Export full BGG table from PostgreSQL to Parquet.

    Args:
        output_path (str): Path where parquet file will be saved.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    print("Connecting to PostgreSQL...")
    engine = create_engine(get_connection_string())

    print("Reading data from DB...")
    df = pd.read_sql("SELECT * FROM public.bgg ORDER BY bgg_id", engine)

    print(f"Loaded {len(df)} rows. Writing to Parquet...")
    df.to_parquet(output_path, engine="pyarrow", index=False)

    print(f"Export completed! File saved as: {output_path}")
