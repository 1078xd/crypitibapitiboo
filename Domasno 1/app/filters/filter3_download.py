from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta

from app.sources.binance_api import fetch_binance_ohlcv
from app.utils.storage import append_records_to_csv
from app.utils.dates import iso_to_date, today_utc


def process_symbol(item, days_back):
    symbol = item["symbol"]
    last_date = item["last_date"]

    # If no existing data then request last X days
    if last_date is None:
        start = today_utc() - timedelta(days=days_back)
    else:
        start = iso_to_date(last_date)

    print(f"Filter3 Downloading {symbol} from {start}")

    # Fetch OHLCV from Binance
    data = fetch_binance_ohlcv(symbol, start)

    if not data:
        return f"Filter3 No data for {symbol}"

    # Write to one CSV
    append_records_to_csv(symbol, data)

    return f"Filter3 Finished {symbol} ({len(data)} rows)"


def update_missing_data(items, days_back):
    print("Filter 3: Parallel downloading\n")

    MAX_WORKERS = 80  
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = {
            executor.submit(process_symbol, item, days_back): item["symbol"]
            for item in items
        }

        for future in as_completed(tasks):
            print(future.result())

    print("Filter 3 finished")
