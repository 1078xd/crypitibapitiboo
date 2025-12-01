import os
import csv
from django.shortcuts import render
from django.http import HttpResponse

from app.pipes.pipe_binance import run_pipe_binance
from app.sources.binance_api import get_binance_symbols

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
DATA_FILE = os.path.join(PROJECT_ROOT, "Domasno 1", "data", "cryptocurrency_binance_all.csv")

def home(request):
    return render(request, "home.html")


def run_pipeline(request):
    message = None

    if request.method == "POST":
        try:
            COIN_LIMIT = 1000
            DAYS_BACK = 3650  # 10 years
            run_pipe_binance(COIN_LIMIT, DAYS_BACK)
            message = "Pipeline finished successfully!"
        except Exception as e:
            message = f"Error: {e}"

    return render(request, "run_pipeline.html", {"message": message})



def data_overview(request):
    if not os.path.exists(DATA_FILE):
        return HttpResponse("No CSV data found. Please run the pipeline first.")

    symbols = set()

    with open(DATA_FILE, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sym = row.get("symbol")

            # SAFETY CHECK: skip None, empty, or invalid symbols
            if sym is None:
                continue

            sym = sym.strip()

            if not sym:
                continue

            # keep ONLY USDT pairs
            if not sym.endswith("USDT"):
                continue

            symbols.add(sym)

    symbols = sorted(symbols)

    return render(request, "data_overview.html", {"symbols": symbols})



def symbol_detail(request, symbol):
    if not os.path.exists(DATA_FILE):
        return HttpResponse("No CSV data found. Run the pipeline first.")

    rows = []
    dates = []
    closes = []

    with open(DATA_FILE, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["symbol"] == symbol:
                rows.append(row)
                dates.append(row["date"])
                closes.append(float(row["close"]))

    return render(request, "symbol_detail.html", {
        "symbol": symbol,
        "rows": rows,
        "dates": dates,
        "closes": closes,
    })
