

import os
import sys
import uuid
import requests
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict
from dotenv import load_dotenv


# Prefect imports
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash

# ----------------- CONFIGURATION --------------
load_dotenv()

# Constants
DB_PATH = "data/crypto.db"
COIN_LIST = [
    "bitcoin", "ethereum", "solana", "cardano", "polkadot",
    "ripple", "dogecoin", "litecoin", "binancecoin", "avalanche",
    "terra-luna", "chainlink", "uniswap", "stellar", "vechain"
    ]

@task(
    name="Extract Prices",
    retries=3,
    retry_delay_seconds=5,
    # Caches result for 1 min so accidental re-runs don't burn API limits
    cache_key_fn=task_input_hash, 
    cache_expiration=timedelta(minutes=1),
    description="Fetches live crypto prices from CoinGecko",
    log_prints=True
)
def extract_prices(coins: List[str]) -> Dict:
    """
    Fetch live cryptocurrency prices from the CoinGecko API.
    Retries automatically on failure.
    """
    logger = get_run_logger()
    api_key = os.getenv("COINGECKO_API_KEY")
    
    url = "https://api.coingecko.com/api/v3/simple/price"
    headers = {"x-cg-demo-api-key": api_key} if api_key else {}
    params = {
        "ids": ",".join(coins),
        "vs_currencies": "usd",
    }

    logger.info(f"Requesting prices for {len(coins)} coins...")
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info("API request successful.")
        return data
        
    except requests.exceptions.HTTPError as e:
        if response.status_code == 429:
            logger.warning("Rate limit hit. Prefect will retry...")
        raise e 

@task(name="Transform Data", description="Normalizes JSON and adds timestamps", log_prints=True)
def transform_prices(raw_json: Dict) -> pd.DataFrame:
    """
    Converts JSON to DataFrame, adds timestamps and metadata.
    """
    logger = get_run_logger()
    
    if not raw_json:
        raise ValueError("Input JSON is empty")

    rows = []
    eat_time = datetime.now(tz=ZoneInfo("Africa/Nairobi"))
    etl_run_id = str(uuid.uuid4())

    for coin, data in raw_json.items():
        rows.append({
            "coin": coin,
            "price_usd": data.get("usd"),
            "fetched_at": eat_time,
            "etl_run_id": etl_run_id
        })

    df = pd.DataFrame(rows)
    logger.info(f"Transformed {len(df)} records. Timestamp: {eat_time}")
    return df

@task(name="Load to SQLite", description="Appends data to SQLite database", log_prints=True)
def load_to_sqlite(df: pd.DataFrame, db_path: str = DB_PATH) -> int:
    """
    Appends data to SQLite database and returns total row count.
    """
    logger = get_run_logger()
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # We use a context manager for safe DB handling
    with sqlite3.connect(db_path) as conn:
        df.to_sql("crypto_prices", conn, if_exists="append", index=False)
        
        # Get total count for verification
        cursor = conn.cursor()
        count = cursor.execute("SELECT COUNT(*) FROM crypto_prices").fetchone()[0]
        logger.info(f"Successfully loaded {len(df)} rows. Total DB rows: {count}")
        return count

# ------------------------------------------
# FLOW
# ------------------------------------------

@flow(name="Crypto Price Ingest", log_prints=True)
def crypto_etl_flow():
    """
    Main Prefect Flow for Crypto ETL.
    """
    logger = get_run_logger()
    logger.info("Starting Crypto ETL Pipeline...")

    # 1. Extract
    raw_data = extract_prices(COIN_LIST)

    # 2. Transform
    df = transform_prices(raw_data)

    # 3. Load
    final_count = load_to_sqlite(df)
    
    logger.info(f"Pipeline finished successfully. Database now has {final_count} records.")

if __name__ == "__main__":
    crypto_etl_flow()