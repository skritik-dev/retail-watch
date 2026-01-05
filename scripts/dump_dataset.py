# Extracts the dataset from the DVC repository and dumps it to a CSV file for the periodic training of the model
# DVC (Data version control): Git for data

import pandas as pd
from sqlalchemy import create_engine
from utils.logger import get_logger

# Config
DB_CONNECTION = "postgresql://retail_user:retail_password@localhost:5432/retail_db"
OUTPUT_FILE = "datasets/products_v1.csv"

logger = get_logger("Dump Dataset")

def extract_data():
    logger.info(" [~] Connecting to Database...")
    engine = create_engine(DB_CONNECTION)
    
    # We select ONLY what we need for training
    #? Tip: Always sort by something (id/date) to ensure 
    #? the 'head' of the file looks the same every time you dump it.
    query = """
    SELECT
        id,
        name,
        price,
        currency,
        in_stock,
        scraped_at 
    FROM products 
    ORDER BY scraped_at ASC;
    """
    
    df = pd.read_sql(query, engine)
    
    if df.empty:
        logger.warning(" [!] Warning: Database is empty! Run the scraper first.")
        return

    logger.info(f" [OK] Extracted {len(df)} rows.")
    
    # Save to CSV
    df.to_csv(OUTPUT_FILE, index=False)
    logger.info(f" [OK] Dataset saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    extract_data()