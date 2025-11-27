from datetime import datetime
import glob
import os
import pandas as pd
from sklearn.model_selection import train_test_split

from extract_db import export_bgg_to_parquet
from preprocessing import cleaning
from model import recommendation_model, recommend_games



def load_latest_parquet(folder : str = "data/exports/") -> pd.DataFrame:
    files = glob.glob(os.path.join(folder, "*.parquet"))
    if not files:
        raise FileNotFoundError("No parquet files found in the specified folder.")

    latest = max(files, key=os.path.getmtime)
    print("Loading the latest file:", latest)

    return pd.read_parquet(latest)

df = load_latest_parquet()



if __name__ == "__main__":
    # Export data from DB
    date_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    export_bgg_to_parquet(f"data/exports/bgg_{date_tag}.parquet")
    # Load the latest exported data + clean it
    df = load_latest_parquet()
    df_cleaned = cleaning(df)
    # Split into train and test
    train_df, test_df = train_test_split(df_cleaned, test_size=0.2, random_state=2137)
    os.makedirs("data/processed/", exist_ok=True)
    train_df.to_parquet("data/processed/bgg_train.parquet")
    test_df.to_parquet("data/processed/bgg_test.parquet")
    # Train the model
    model_pipeline = recommendation_model(train_df)
    # Example recommendation
    recommended_titles = recommend_games(
        train=train_df,
        pipeline=model_pipeline,
        favorite_titles=["Scythe", "7 Wonders Duel", "Terra Mystica"],
        top_k=1
    )
    print("Recommended titles:", recommended_titles)