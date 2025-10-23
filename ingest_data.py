# ingest_data.py
import certifi
import os
# ensure requests uses certifi CA bundle for SSL verification on Windows
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

import time
import requests
import pandas as pd
import json
from sf_utils import write_df_to_snowflake
from dotenv import load_dotenv

load_dotenv()

ALPHA_KEY = os.environ.get("ALPHA_VANTAGE_KEY")
NEWS_KEY = os.environ.get("NEWSAPI_KEY")

# -------- Structured Data: Stock Prices --------
def fetch_stock(symbol):
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": "5min",
        "apikey": ALPHA_KEY
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"Error fetching stock for {symbol}: {e}")
        return pd.DataFrame()

    time_key = None
    # find key like "Time Series (5min)"
    for k in data.keys():
        if "Time Series" in k:
            time_key = k
            break

    if not time_key or time_key not in data:
        print(f"No time series returned for {symbol}. Response snippet: {str(data)[:300]}")
        return pd.DataFrame()

    series = data[time_key]
    rows = []
    for ts, vals in series.items():
        try:
            rows.append({
                "SYMBOL": symbol,
                "TS": pd.to_datetime(ts),
                "OPEN": float(vals.get("1. open", 0)),
                "HIGH": float(vals.get("2. high", 0)),
                "LOW": float(vals.get("3. low", 0)),
                "CLOSE": float(vals.get("4. close", 0)),
                "VOLUME": int(vals.get("5. volume", 0))
            })
        except Exception:
            # skip malformed row
            continue
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("TS").reset_index(drop=True)
    return df

def ingest_stocks(symbols):
    for i, s in enumerate(symbols):
        print(f"Fetching stock data for {s} ({i+1}/{len(symbols)})")
        df = fetch_stock(s)
        if not df.empty:
            # ensure index is RangeIndex and columns uppercase (already uppercase in fetch)
            df.reset_index(drop=True, inplace=True)
            write_df_to_snowflake(df, "STOCK_PRICES")
        else:
            print(f"No data to write for {s}")
        # rate limit for Alpha Vantage free tier: ~5 calls/minute
        if i < len(symbols) - 1:
            time.sleep(12)

def fetch_news(query="stocks OR markets", max_pages=2):
    all_articles = []
    base_url = "https://newsapi.org/v2/everything"
    for page in range(1, max_pages + 1):
        params = {
            "q": query,
            "language": "en",
            "pageSize": 100,
            "page": page,
            "apiKey": NEWS_KEY
        }
        try:
            # r = requests.get(base_url, params=params, timeout=30)
            r = requests.get(base_url, params=params, timeout=30, verify=False)
            r.raise_for_status()
            resp = r.json()
        except Exception as e:
            print(f"Error fetching news page {page}: {e}")
            break

        articles = resp.get("articles", [])
        if not articles:
            break
        all_articles.extend(articles)
        # polite pause
        time.sleep(1)
    return all_articles

def articles_to_df(articles):
    rows = []
    for a in articles:
        rows.append({
            "SOURCE": (a.get("source") or {}).get("name"),
            "AUTHOR": a.get("author"),
            "TITLE": a.get("title"),
            "DESCRIPTION": a.get("description"),
            "CONTENT": a.get("content"),
            "URL": a.get("url"),
            "PUBLISHED_AT": pd.to_datetime(a.get("publishedAt")) if a.get("publishedAt") else None,
            "RAW": json.dumps(a)
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.reset_index(drop=True)
    return df

def ingest_news():
    print("Fetching news articles...")
    articles = fetch_news()
    if not articles:
        print("No articles fetched.")
        return
    df = articles_to_df(articles)
    if not df.empty:
        write_df_to_snowflake(df, "NEWS_ARTICLES")
    else:
        print("No news rows to write.")

# -------- Main Function --------
if __name__ == "__main__":
    # change symbols or query here if you want
    symbols = ["AAPL", "MSFT", "GOOGL"]
    ingest_stocks(symbols)
    ingest_news()
