Perfect! Here’s a fully structured `exercises.md` ready for learners. It includes **all exercises, hints, starter code snippets, and expected outputs**. You can place this in your repo for hands-on practice.

---

# 🏋️ Crypto ETL & Prefect Exercises

## **Overview**

This set of exercises will guide you through **fetching cryptocurrency data from CoinGecko**, transforming it, storing it in SQLite, and orchestrating the workflow using Prefect 2.x. Exercises progress from **beginner → advanced**.

---

## **Setup Instructions**

1. Clone repo & activate virtual environment:

```bash
git clone <repo-url>
cd workflow-orchestration-demo
source .workflow-orchestration-demo/bin/activate
pip install -r requirements.txt
```

2. Start Prefect Orion server:

```bash
prefect server start
```

UI: [http://127.0.0.1:4200](http://127.0.0.1:4200)

3. Set Prefect API URL:

```bash
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

4. Add `.env` with your CoinGecko API key:

```env
COINGECKO_API_KEY=YOUR_API_KEY
```

---

## **Exercise 1 — Basic API Fetch**

**Goal:** Fetch live Ethereum price in USD & CAD.

```bash
curl --request GET \
  --url 'https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd,cad&include_last_updated_at=true' \
  --header 'x-cg-demo-api-key: <YOUR_API_KEY>'
```

**Hints:**

* `ids=`: list of coins, comma-separated
* `vs_currencies=`: currencies, comma-separated
* `include_last_updated_at=true`: returns UNIX timestamp

**Variation:** Fetch Litecoin or include `market_cap`.

---

## **Exercise 2 — Multiple Coins**

Fetch **Bitcoin, Ethereum, Solana** in USD and EUR:

```bash
curl --request GET \
  --url 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana&vs_currencies=usd,eur&include_last_updated_at=true' \
  --header 'x-cg-demo-api-key: <YOUR_API_KEY>'
```

**Variation:** Add Cardano and Dogecoin. Include `market_cap` and `24h_vol`.

---

## **Exercise 3 — JSON → DataFrame**

**Goal:** Convert JSON to table:

Columns: `coin | price_usd | price_eur | last_updated_at`

**Starter Python:**

```python
import pandas as pd
import requests
from datetime import datetime
import uuid

response = requests.get(api_url, headers=headers).json()
rows = []
etl_run_id = str(uuid.uuid4())
for coin, data in response.items():
    rows.append({
        "coin": coin,
        "price_usd": data.get("usd"),
        "price_eur": data.get("eur"),
        "last_updated_at": datetime.fromtimestamp(data.get("last_updated_at")),
        "etl_run_id": etl_run_id
    })
df = pd.DataFrame(rows)
print(df)
```

**Variation:** Add KES price column using a fixed conversion rate.

---

## **Exercise 4 — Dynamic Coin Selection**

* Create a Python list of coins.
* Fetch only coins **above $100 USD**.
* Display top 5 coins by price.

**Variation:** Filter by market cap > 1B USD.

---

## **Exercise 5 — Load to SQLite**

**Goal:** Store data locally.

```python
import sqlite3
df.to_sql("crypto_prices", sqlite3.connect("data/crypto.db"), if_exists="append", index=False)
```

**Check table:**

```python
pd.read_sql("SELECT * FROM crypto_prices ORDER BY last_updated_at DESC LIMIT 5", sqlite3.connect("data/crypto.db"))
```

**Variation:** Prevent duplicate rows using `etl_run_id`.

---

## **Exercise 6 — Error Handling & Retries**

* Simulate invalid coin ID: `ids=invalidcoin`
* Add retry logic in Prefect task:

```python
@task(retries=3, retry_delay_seconds=5)
def extract_prices(coins):
    ...
```

**Variation:** Handle `HTTP 429` rate limits; cache API responses for 1 min.

---

## **Exercise 7 — Multi-Currency Prices**

* Fetch Ethereum & Bitcoin in USD, EUR, CAD, GBP, JPY.
* Normalize DataFrame:

```text
coin | currency | price
```

**Variation:** Include `24h_change` in DataFrame.

---

## **Exercise 8 — Prefect Orchestration**

* Create a Prefect flow for Extract → Transform → Load.
* Run:

```bash
python orchestrated-etl/prefect_crypto_etl.py
```

* Observe tasks and flow runs in UI.

**Variation:** Schedule hourly runs, push CSV artifacts, or add colored logging.

---

## **Exercise 9 — Data Visualization**

* Plot price trends for 3 coins over 7 days:

```python
import matplotlib.pyplot as plt

df = pd.read_sql("SELECT * FROM crypto_prices", sqlite3.connect("data/crypto.db"))
df[df['coin']=='bitcoin'].plot(x='last_updated_at', y='price_usd')
plt.show()
```

**Variation:** Plot multiple currencies on the same chart; include ETL run ID in hover tooltip.

---

