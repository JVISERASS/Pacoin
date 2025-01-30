import pandas as pd
import numpy as np
import os
from typing import Tuple, Union

def missing_data(df: pd.DataFrame) -> Tuple[int, pd.Series]:
    """Calculate the percentage of missing data in the DataFrame."""
    missing = df.isnull().sum()
    total = missing.sum()
    return total, 100 * missing / df.shape[0]

def load_data(path: str) -> pd.DataFrame:
    """Load the data from the specified path."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"File '{path}' not found.")
    data = pd.read_csv(path, parse_dates=['datetime'])
    return data

def clean_data(df: pd.DataFrame, method: str = 'drop') -> pd.DataFrame:
    """Clean the data by handling missing values using the specified method."""
    if method == 'drop':
        return df.dropna()
    elif method == 'fill':
        return df.fillna(method='ffill')
    elif method == 'mean':
        return df.fillna(df.mean())
    elif method == 'median':
        return df.fillna(df.median())
    elif method == 'mode':
        return df.fillna(df.mode().iloc[0])
    elif method == 'zero':
        return df.fillna(0)
    elif method == 'moving_avg':
        return df.fillna(df.rolling(2).mean())
    else:
        raise ValueError(f"Invalid method '{method}'. Use 'drop', 'fill', 'mean', 'median', 'mode', 'zero', or 'moving_avg'.")

def missing_values_days(data: pd.DataFrame) -> pd.DatetimeIndex:
    """Return the dates that have missing values."""
    return data[data.isnull().any(axis=1)].index

def drop_consecutive_missing_days(data: pd.DataFrame, threshold: int = 5) -> pd.DataFrame:
    """Drop rows if there are `threshold` or more consecutive days with missing values."""
    data = data.copy()
    data['missing'] = data.isnull().any(axis=1)
    data['group'] = (~data['missing']).cumsum()
    consecutive_missing = data.groupby('group').filter(lambda x: x['missing'].sum() >= threshold)
    return data.drop(consecutive_missing.index).drop(columns=['missing', 'group'])

def run_data_processing(path: str, output_path: str = 'data/ETHUSDT.csv') -> pd.DataFrame:
    """Main function to execute the data processing process."""
    data = load_data(path)
    
    print("Data loaded successfully")
    print(f"Data shape: {data.shape}")
    
    total_missing, missing_percent = missing_data(data)
    print(f"Total missing data: {total_missing}")
    print("Percentage of missing data:")
    print(missing_percent)

    missing_val_dates = missing_values_days(data)
    print("Dates with missing values:")
    print(missing_val_dates)

    data = drop_consecutive_missing_days(data, threshold=5)
    print("Rows with 5 or more consecutive missing days dropped successfully")

    data = clean_data(data, method='moving_avg')
    print("Missing values filled with the moving average successfully")

    total_missing, missing_percent = missing_data(data)
    print(f"Total missing data after cleaning: {total_missing}")

    data.to_csv(output_path, index=False)
    print(f"Data saved successfully to {output_path}")
    return data

if __name__ == "__main__":
    input_path = 'data/btc/BTCUSD.csv'
    output_path = 'data/ETHUSDT.csv'
    cleaned_data = run_data_processing(input_path, output_path)