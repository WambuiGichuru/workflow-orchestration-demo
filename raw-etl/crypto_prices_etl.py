import os
import sys
import uuid
import time
import logging
import sqlite3
import requests
import pandas as pd
from typing import List, Dict
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# Modules for a professional looking CLI
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.logging import RichHandler

# ----------------- CONFIGURATION --------------
load_dotenv()
console = Console()

# Setup logging
logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console, markup=True)]
)
log = logging.getLogger("etl_logger")

# Check for API Key (Graceful exit if missing)
API_KEY = os.getenv("COINGECKO_API_KEY")
if not API_KEY:
    # Warning only: CoinGecko works without a key for limited requests
    log.warning("[yellow]No COINGECKO_API_KEY found in .env. Running in Public mode (limited rates).[/yellow]")

COIN_LIST = [
    "bitcoin", "ethereum", "solana", "cardano", "polkadot",
    "ripple", "dogecoin", "litecoin", "binancecoin", "avalanche",
    "terra-luna", "chainlink", "uniswap", "stellar", "vechain"
]

# ------------------------------------------
# EXTRACT STEP
# ------------------------------------------
def extract_prices(coins: List[str]) -> Dict:
    """
    Fetch live cryptocurrency prices from the CoinGecko API.
    """
    url = "https://api.coingecko.com/api/v3/simple/price"
    
    headers = {"x-cg-demo-api-key": API_KEY} if API_KEY else {}
    
    params = {
        "ids": ",".join(coins),
        "vs_currencies": "usd",
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status() # Raises error for 4xx/5xx codes
        return response.json()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 429:
            raise Exception("Rate Limit Exceeded. Please wait or use an API Key.")
        raise e
    except Exception as e:
        raise Exception(f"Connection failed: {e}")

# ------------------------------------------
# TRANSFORM STEP
# ------------------------------------------
def transform_prices(raw_json: Dict) -> pd.DataFrame:
    """
    Converts JSON to DataFrame, adds timestamps and metadata.
    """
    rows = []
    # Using Nairobi time as requested
    eat_time = datetime.now(tz=ZoneInfo("Africa/Nairobi"))
    etl_run_id = str(uuid.uuid4()) 

    for coin, data in raw_json.items():
        rows.append({
            "coin": coin,
            "price_usd": data.get("usd"),
            "fetched_at": eat_time, # Renamed for clarity
            "etl_run_id": etl_run_id
        })

    df = pd.DataFrame(rows)
    
    # Handle case where API returns partial data or empty
    if df.empty:
        raise ValueError("API returned no data to transform.")
        
    return df

# ------------------------------------------
# LOAD STEP
# ------------------------------------------
def load_to_sqlite(df: pd.DataFrame, db_path="data/crypto.db"):
    """
    Appends data to SQLite database.
    """
    os.makedirs("data", exist_ok=True)
    
    try:
        with sqlite3.connect(db_path) as conn:
            # if_exists="append" allows building history over time
            df.to_sql("crypto_prices", conn, if_exists="append", index=False)
            
            # Optional: verify count
            cursor = conn.cursor()
            count = cursor.execute("SELECT COUNT(*) FROM crypto_prices").fetchone()[0]
            return count
    except Exception as e:
        raise Exception(f"Database error: {e}")

# ------------------------------------------
# MAIN ORCHESTRATION
# ------------------------------------------
def run_etl():
    console.print(Panel.fit("[bold blue]CRYPTO PIPELINE EXECUTION[/bold blue]", subtitle="Live Data Ingest"))

    # --- STEP 1: EXTRACT ---
    raw_data = {}
    with console.status("[bold cyan]STEP 1: Fetching prices from CoinGecko...", spinner="dots"):
        try:
            raw_data = extract_prices(COIN_LIST)
            log.info(f"[green]Extract Success.[/green] Retrieved prices for {len(raw_data)} coins.")
        except Exception as e:
            log.error(f"[red]Extract Failed:[/red] {e}")
            sys.exit(1)

    # --- STEP 2: TRANSFORM ---
    df = pd.DataFrame()
    with console.status("[bold cyan]STEP 2: Normalizing data...", spinner="dots"):
        try:
            df = transform_prices(raw_data)
            log.info(f"[green]Transform Success.[/green] Processed {len(df)} records.")
        except Exception as e:
            log.error(f"[red]Transform Failed:[/red] {e}")
            sys.exit(1)

    # Preview Table
    console.print("\n[yellow]Preview of Batch:[/yellow]")
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Coin", style="cyan")
    table.add_column("Price (USD)", justify="right", style="green")
    table.add_column("Timestamp (EAT)", style="dim")

    for _, row in df.iterrows():
        table.add_row(
            row['coin'].title(), 
            f"${row['price_usd']:,.2f}", 
            row['fetched_at'].strftime("%H:%M:%S")
        )
    console.print(table)

    # --- STEP 3: LOAD ---
    with console.status("[bold cyan]STEP 3: Saving to SQLite...", spinner="dots"):
        try:
            total_rows = load_to_sqlite(df)
            log.info(f"[green]Load Success.[/green] Database now holds {total_rows} total records.")
        except Exception as e:
            log.error(f"[red]Load Failed:[/red] {e}")
            sys.exit(1)

    console.print(Panel("[bold green]ETL JOB COMPLETED[/bold green]", expand=False))

if __name__ == "__main__":
    try:
        run_etl()
    except KeyboardInterrupt:
        console.print("\n[bold red]Pipeline stopped by user.[/bold red]")