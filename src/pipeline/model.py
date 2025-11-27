import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline



def extract_feature_columns(train: pd.DataFrame):
    """
    Extracts numerical and binary feature columns from the training DataFrame.

    Args:
        train (pd.DataFrame): The training dataset.

    Returns:
        tuple: A tuple containing two lists - numerical columns and binary columns.
    """
    binary_cols = [c for c in train.columns if c.startswith(("categories_", "mechanics_", "types_"))]

    numerical_cols = [
        "release_year", "min_players", "max_players", "min_play_time", 
        "max_play_time", "min_age", "std_deviation", "weight", 
        "avg_rating", "no_of_ratings",
        "comments", "fans", "page_views", "overall_rank", 
        "strategy_rank", "all_time_plays", "this_month_plays",
        "own_count", "prev_owned_count", "for_trade_count",
        "want_in_trade_count", "wishlist_count"
    ]

    return numerical_cols, binary_cols



def recommendation_model(train: pd.DataFrame, n_neighbors=50) -> Pipeline:
    """
    Builds and trains a recommendation model using the BGG dataset.

    Args:
        train (pd.DataFrame): The training dataset.
        n_neighbors (int): The number of neighbors for the KNN model.

    Returns:
        Pipeline: The trained recommendation model pipeline.
    """
    
    numerical_cols, binary_cols = extract_feature_columns(train)

    X_train = train[numerical_cols + binary_cols]

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), numerical_cols),
            ("bin", "passthrough", binary_cols)
        ]
    )

    knn = NearestNeighbors(
        n_neighbors=n_neighbors,
        metric="cosine",
        algorithm="brute"
    )

    pipeline = Pipeline(steps=[
        ("preprocess", preprocessor),
        ("knn", knn)
    ])

    pipeline.fit(X_train)

    return pipeline



def recommend_games(train: pd.DataFrame, 
                    pipeline: Pipeline, 
                    favorite_titles: str | list[str], top_k: int = 10):
    """
    Recommends games based on a user's favorite titles.

    Usage example:
    ```python
    recommended_titles = recommend_games(
        train=train_df,
        pipeline=model_pipeline,
        favorite_titles=["Scythe", "7 Wonders Duel", "Terra Mystica"],
        top_k=1
    )

    Args:
        train (pd.DataFrame): The training dataset.
        pipeline (Pipeline): The trained recommendation model pipeline.
        favorite_titles (list): A list of user's favorite game titles.
        top_k (int): The number of top recommendations to return.

    Returns:
        list: A list of recommended game titles.
    """
    numerical_cols, binary_cols = extract_feature_columns(train)
    
    if isinstance(favorite_titles, str):
        favorite_titles = [favorite_titles]

    user_rows = train[train["title"].isin(favorite_titles)]

    if user_rows.empty:
        raise ValueError("None of the provided game titles were found in the data.")
    user_indices = user_rows.index.tolist()

    user_vectors = pipeline.named_steps["preprocess"].transform(
        train.loc[user_indices, numerical_cols + binary_cols]
    )
    user_profile = user_vectors.mean(axis=0).reshape(1, -1)
    _, indices = pipeline.named_steps["knn"].kneighbors(
        user_profile,
        n_neighbors=top_k + len(user_indices)
    )

    recommended = train.iloc[indices[0]]
    recommended = recommended[~recommended.index.isin(user_indices)]

    return recommended["title"].head(top_k).tolist()
