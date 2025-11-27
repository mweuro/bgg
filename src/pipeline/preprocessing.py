import numpy as np
import os
import pandas as pd
from sklearn.model_selection import train_test_split



def cleaning(data: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the BGG dataset by:
    - Removing unnecessary columns
    - Normalizing the 'weight' column
    - Creating a binary 'is_strategy_rank' column
    - One-hot encoding categorical columns: 'types', 'categories', 'mechanics'

    Args:
        data (pd.DataFrame): The raw BGG dataset.
    
    Returns:
        pd.DataFrame: The cleaned dataset.
    """
    # Remove unnecessary columns
    data = data.copy()
    data.drop(columns=['id', 
                       'has_parts_count', 
                       'want_parts_count', 
                       'created_at', 
                       'updated_at'], inplace=True)
    
    # Game weight -> convert to numeric
    def normalize_weight(w: str) -> float:
        n, d = w.split('/')
        return float(n.strip()) / float(d.strip())
    data['weight'] = data['weight'].apply(normalize_weight).astype(np.float32)
    # Strategy rank
    data['is_strategy_rank'] = data['strategy_rank'].notna().astype(int)
    data.fillna({'strategy_rank': 0}, inplace=True)
    # Categories -> one-hot encoding
    cols = ["types", "categories", "mechanics"]
    for col in cols:
        dummies = data[col].str.get_dummies(sep=",").add_prefix(f"{col}_")
        data = pd.concat([data, dummies], axis=1)
    data.drop(columns=cols, inplace=True)
    data.dropna(inplace=True)

    return data
