import os
import time
import json
import pandas as pd
import requests
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from pathlib import Path

# === Load environment variables ===
load_dotenv()
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
ES_API_KEY = os.getenv("ES_API_KEY")
ES_CLOUD_ENDPOINT = os.getenv("ES_CLOUD_ENDPOINT")

# === Constants ===
CHECKPOINT_FILE = "checkpoint.txt"
DATA_DIR = Path("data_batches")
DATA_DIR.mkdir(exist_ok=True)
TICKERS_FILE = "tickers.csv"
INDEX_NAME = "financial_data"
MAX_CALLS_PER_MINUTE = 60
RETRY_LIMIT = 3

# === Elasticsearch client ===
es = Elasticsearch(ES_CLOUD_ENDPOINT, api_key=ES_API_KEY, request_timeout=60, verify_certs=True)

# === Utility Functions ===
def read_checkpoint():
    if Path(CHECKPOINT_FILE).exists():
        with open(CHECKPOINT_FILE) as f:
            return f.read().strip()
    return ""

def write_checkpoint(ticker):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(ticker)

def rate_limit_guard(start_time, call_count):
    if call_count >= MAX_CALLS_PER_MINUTE:
        elapsed = time.time() - start_time
        if elapsed < 60:
            time.sleep(60 - elapsed)
        return time.time(), 0
    return start_time, call_count

def convert_ints_to_floats(obj):
    if isinstance(obj, dict):
        return {k: convert_ints_to_floats(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_ints_to_floats(i) for i in obj]
    elif isinstance(obj, int):
        return float(obj)
    return obj

def clean_document(data):
    if isinstance(data, dict):
        return {k: clean_document(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_document(v) for v in data]
    elif isinstance(data, str) and data.upper() in ["N/A", "NA", "NULL", "-"]:
        return None
    return data

def fetch_finnhub_data(endpoint, params):
    for attempt in range(RETRY_LIMIT):
        try:
            url = f"https://finnhub.io/api/v1/{endpoint}"
            params["token"] = FINNHUB_API_KEY
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                return resp.json()
            time.sleep(2 ** attempt)
        except Exception:
            time.sleep(2 ** attempt)
    return None

def index_to_es(symbol, data):
    try:
        es.index(index=INDEX_NAME, id=symbol, body=data)
    except Exception as e:
        print(f"❌ Elasticsearch indexing failed for {symbol}: {e}")
        try:
            es.index(index=f"{INDEX_NAME}_failed", id=symbol, body=data)
            print(f"⚠️ Stored bad document in {INDEX_NAME}_failed")
        except Exception as err:
            print(f"🚨 Even backup indexing failed for {symbol}: {err}")

# === Main Ingestion Pipeline ===
def run_pipeline():
    df = pd.read_csv(TICKERS_FILE)
    if "symbol" not in df.columns:
        df.columns = df.columns.str.lower()
    tickers = df["symbol"].tolist()

    start_from = read_checkpoint()
    if start_from in tickers:
        tickers = tickers[tickers.index(start_from)+1:]

    batch_data = []
    start_time = time.time()
    call_count = 0

    for symbol in tickers:
        print(f"\n▶️ Processing: {symbol}")
        start_time, call_count = rate_limit_guard(start_time, call_count)

        profile = fetch_finnhub_data("stock/profile2", {"symbol": symbol})
        quote = fetch_finnhub_data("quote", {"symbol": symbol})
        financials = fetch_finnhub_data("stock/financials-reported", {"symbol": symbol})
        peers = fetch_finnhub_data("stock/peers", {"symbol": symbol})
        metrics = fetch_finnhub_data("stock/metric", {"symbol": symbol, "metric": "all"})
        call_count += 5

        if not profile or not quote:
            print(f"⚠️ Skipping {symbol} due to missing data.")
            continue

        document = {
            "symbol": symbol,
            "profile": profile,
            "quote": quote,
            "financials": financials,
            "peers": peers,
            "metrics": metrics,
            "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ')
        }

        document = clean_document(document)
        document = convert_ints_to_floats(document)
        batch_data.append(document)

        index_to_es(symbol, document)
        write_checkpoint(symbol)

        # === Save to Parquet every 25 tickers (flattened version) ===
        if len(batch_data) % 25 == 0:
            flat_data = [
                {
                    "symbol": d.get("symbol"),
                    "timestamp": d.get("timestamp"),
                    "sector": d.get("profile", {}).get("finnhubIndustry"),
                    "price": d.get("quote", {}).get("c"),
                    "pe_ratio": d.get("metrics", {}).get("metric", {}).get("peInclExtraTTM")
                }
                for d in batch_data
            ]
            df = pd.DataFrame(flat_data)
            file_path = DATA_DIR / f"batch_{symbol}.parquet"
            df.to_parquet(file_path, index=False)
            print(f"📦 Saved batch to {file_path}")
            batch_data.clear()

    # === Save final batch if any ===
    if batch_data:
        flat_data = [
            {
                "symbol": d.get("symbol"),
                "timestamp": d.get("timestamp"),
                "sector": d.get("profile", {}).get("finnhubIndustry"),
                "price": d.get("quote", {}).get("c"),
                "pe_ratio": d.get("metrics", {}).get("metric", {}).get("peInclExtraTTM")
            }
            for d in batch_data
        ]
        df = pd.DataFrame(flat_data)
        file_path = DATA_DIR / f"batch_final.parquet"
        df.to_parquet(file_path, index=False)
        print(f"📦 Final batch saved to {file_path}")

# === Run Script ===
if __name__ == "__main__":
    run_pipeline()
