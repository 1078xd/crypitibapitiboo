import os
import csv

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
APP_DIR = os.path.dirname(CURRENT_DIR)
PROJECT_ROOT = os.path.dirname(APP_DIR)

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)

MASTER_FILE = os.path.join(DATA_DIR, "cryptocurrency_binance_all.csv")

def append_records_to_csv(symbol, records):
    file_exists = os.path.exists(MASTER_FILE)

    with open(MASTER_FILE, "a", newline="") as f:
        w = csv.writer(f)

        if not file_exists:
            w.writerow([
                "date", "symbol", "open", "high", "low", "close", "volume"
            ])

        for r in records:
            w.writerow([
                r["date"],
                symbol,
                r["open"],
                r["high"],
                r["low"],
                r["close"],
                r["volume"],
            ])
