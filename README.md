Absolutely! Here's a **comprehensive, beginner-friendly documentation** for setting up your **Crypto ETL pipeline with Prefect 2.x**, including environment setup, flow execution, and UI usage. You can use this as a `README.md`.

---

#  Crypto ETL Pipeline Setup Guide

## Overview

This pipeline fetches live cryptocurrency prices from the CoinGecko API, transforms the data, and stores it in a local SQLite database. It is orchestrated using **Prefect 2.x (Orion)**, which handles scheduling, execution, and logging.

The pipeline includes:

* **Extract**: Fetches live prices for a list of coins.
* **Transform**: Normalizes data, adds timestamp and ETL run ID.
* **Load**: Appends data to a SQLite database.

---

## Prerequisites

* Python ≥ 3.10
* `pip` installed
* macOS/Linux or Windows with WSL
* CoinGecko API key (for demo purposes; optional for free API)

---

## Step 1 — Clone the repository

```bash
git clone <your-repo-url>
cd workflow-orchestration-demo
```

---

## Step 2 — Create a virtual environment

Using `uv`:

```bash
uv venv .workflow-orchestration-demo
```

Activate the environment:

```bash
source .workflow-orchestration-demo/bin/activate
```

---

## Step 3 — Install dependencies

```bash
pip install -r requirements.txt
```

Dependencies include:

* `prefect`
* `pandas`
* `requests`
* `python-dotenv`
* `sqlite3` (standard library)
* `zoneinfo` (Python ≥ 3.9)

---

## Step 4 — Configure environment variables

Create a `.env` file in `orchestrated-etl` or `raw-etl` folder:

```env
COINGECKO_API_KEY=YOUR_API_KEY_HERE
```

**Note:** Free CoinGecko API may work without a key, but it is recommended to include one for demo.

---

## Step 5 — Start Prefect Server (Orion)

Prefect 2.x includes the **server (API + UI)**:

```bash
prefect server start
```

* API: `http://127.0.0.1:4200/api`
* UI Dashboard: `http://127.0.0.1:4200`

Keep this terminal open.

---

## Step 6 — Configure Prefect CLI to point to your server

In a separate terminal (or same, new shell):

```bash
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

Check:

```bash
prefect config view | grep PREFECT_API_URL
# Output should be: http://127.0.0.1:4200/api
```

---

## Step 7 — Run the ETL flow

Execute the flow script:

```bash
python orchestrated-etl/prefect_crypto_etl.py
```

**What happens:**

1. Extract: fetch live crypto prices
2. Transform: normalize and add timestamp & ETL run ID
3. Load: save data to `data/crypto.db` (SQLite)

---

## Step 8 — View results in Prefect UI

Open:

```
http://127.0.0.1:4200
```

You will see:

* **Flow Runs**: each time you executed the pipeline
* **Task Runs**: extract, transform, load
* **Logs**: real-time output from tasks
* **Artifacts**: optional outputs like DataFrame previews

---

## Step 9 — Optional: Create a deployment

To schedule or trigger flows from UI:

```bash
prefect deployment build orchestrated-etl/prefect_crypto_etl.py:crypto_etl_flow -n crypto-demo
prefect deployment apply crypto_etl_flow-crypto-demo.yaml
```

Trigger from CLI:

```bash
prefect deployment run "crypto_etl_flow/crypto-demo"
```

---

## Step 10 — Inspect SQLite Database

```bash
sqlite3 data/crypto.db
```

Check table contents:

```sql
SELECT * FROM crypto_prices ORDER BY fetched_at DESC LIMIT 10;
```

---

## Notes / Best Practices

* **ETL Run ID** ensures each pipeline execution is uniquely identifiable.
* **EAT Timestamp** ensures all records are timezone-aware.
* **Prefect 2.x** automatically handles retries and logging.
* **Adding Coins**: simply modify the `COIN_LIST` in the flow.
* **Avoid port conflicts**: if 4200 is busy, use `--port 2400` when starting server and update `PREFECT_API_URL`.

---

## References

* [Prefect 2.x Documentation](https://docs.prefect.io/)
* [CoinGecko API](https://www.coingecko.com/en/api)
* [Python sqlite3 module](https://docs.python.org/3/library/sqlite3.html)

---
