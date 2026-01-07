import pandas as pd
import numpy as np
from pathlib import Path
from utils.logger import get_logger

logger = get_logger("Features")

BASE_DIR = Path(__file__).resolve().parent.parent  
INPUT_FILE = BASE_DIR / "datasets/products_v1.csv"
OUTPUT_FILE = BASE_DIR / "datasets/training_data.csv"

def generate_features():
    logger.info(" [~] Feature engineering started.")
    
    # Load Data
    df = pd.read_csv(INPUT_FILE)
    
    # Convert timestamp to datetime objects
    df['scraped_at'] = pd.to_datetime(df['scraped_at'])
    
    # We must sort by Product and Time to calculate lags correctly
    df = df.sort_values(by=['id', 'scraped_at'])
    
    # Create Lag Features
    # We group by 'id' so we don't mix data between Product A and Product B
    
    # Feature: Price Change vs Previous Scrape
    df['prev_price'] = df.groupby('id')['price'].shift(1)
    df['price_diff'] = df['price'] - df['prev_price']
    df['price_diff_percent'] = (df['price_diff'] / df['prev_price']).fillna(0)
    
    # Feature: Rolling Average Price - This smooths out noise
    df['price_rolling_3'] = df.groupby('id')['price'].transform(
        lambda x: x.rolling(window=3, min_periods=1).mean()
    )
    
    # Feature: Volatility (Standard Deviation of price)
    df['price_std_3'] = df.groupby('id')['price'].transform(
        lambda x: x.rolling(window=3, min_periods=1).std()
    ).fillna(0)
    
    # TODO: Predict if stock will run out in the NEXT step (Given everything up to time t, can I anticipate t+1?)
    # We look into the future (-1) to create labels
    df['next_stock_status'] = df.groupby('id')['in_stock'].shift(-1)
    
    # Target = 1, if currently in stock but next state is out of stock
    df['target_stock_out'] = np.where(
        (df['in_stock'] == True) & (df['next_stock_status'] == False), 
        1, 0
    )
    
    # Clean up the last row of every product will have NaN target (we don't know the future)
    df = df.dropna(subset=['prev_price', 'next_stock_status'])
    
    # Select final columns for training
    features = [
        'price', 'price_diff', 'price_diff_percent', 'price_rolling_3', 'price_std_3', 'target_stock_out'
    ]
    
    final_df = df[features]
    
    logger.info(f" [OK] Generated {len(final_df)} training samples.")
    logger.info(f" [OK] Positive Samples (Stock-outs): {final_df['target_stock_out'].sum()}")
    
    final_df.to_csv(OUTPUT_FILE, index=False)
    logger.info(f" [OK] Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_features()