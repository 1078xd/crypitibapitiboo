import requests
import time
from app.utils.dates import date_to_iso
from datetime import datetime

BASE = "https://api.binance.com"

def safe_get(url, params=None, retries=5):
    # Safe request with retry + rate limit handling.
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=10)

            if r.status_code == 429:  # rate limit
                print("Rate limited. Waiting...")
                time.sleep(2)
                continue

            if r.status_code != 200:
                print(f"Error {r.status_code}: {r.text}")
                time.sleep(1)
                continue

            return r.json()

        except Exception as e:
            print(f"Exception: {e}")
            time.sleep(1)

    print("Failed after retries.")
    return None


def fetch_binance_symbols():
    # Returns list of valid USDT spot trading pairs.
    url = BASE + "/api/v3/exchangeInfo"
    data = safe_get(url)

    if not data:
        return []

    symbols = []
    for s in data["symbols"]:
        if s["status"] != "TRADING":
            continue
        if s["quoteAsset"] != "USDT":
            continue
        if s["isSpotTradingAllowed"] is not True:
            continue

        symbols.append(s["symbol"])  # e.g. "BTCUSDT"

    return symbols


def get_binance_symbols(limit=None):
    symbols = fetch_binance_symbols()
    if limit:
        return symbols[:limit]
    return symbols


def fetch_binance_ohlcv(symbol, start_date):
    # Returns daily OHLCV data from 'start_date' → now.
    url = BASE + "/api/v3/klines"

    start_ms = int(datetime.combine(start_date, datetime.min.time()).timestamp() * 1000)

    params = {
        "symbol": symbol,
        "interval": "1d",
        "startTime": start_ms,
        "limit": 1000  # max per request
    }

    output = []

    while True:
        data = safe_get(url, params)
        if not data:
            break

        for c in data:
            ts = c[0] // 1000
            output.append({
                "date": date_to_iso(ts),
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
                "volume": float(c[5])
            })

        if len(data) < 1000:
            break

        # next batch
        params["startTime"] = data[-1][0] + 1
        time.sleep(0.4)
        
    return output
    
def fetch_binance_24h_stats(symbol):
    # Fetch 24h ticker statistics for a symbol.

    # Returns a dict with:
    #   - last_price
    #   - high_24h
    #   - low_24h
    #   - volume_24h
    #   - quote_volume_24h
    #   - raw (full Binance response)
    url = BASE + "/api/v3/ticker/24hr"
    params = {"symbol": symbol}

    data = safe_get(url, params=params)
    if not data:
        return None

    try:
        return {
            "last_price": float(data["lastPrice"]),
            "high_24h": float(data["highPrice"]),
            "low_24h": float(data["lowPrice"]),
            "volume_24h": float(data["volume"]),
            "quote_volume_24h": float(data["quoteVolume"]),  # quote asset volume (USDT)
            "raw": data,
        }
    except (KeyError, ValueError):
        # If something is missing or malformed, just skip stats for this symbol
        return None
