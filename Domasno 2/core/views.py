import os
import csv
import pandas as pd

from django.shortcuts import render
from django.http import HttpResponse
from django.core.paginator import Paginator

from core.models import CryptoOHLCV, MarketSnapshot
from core.utils.queryset_to_df import queryset_to_df
from core.utils.timeframes import resample_timeframe
from core.indicators.indicators import compute_indicators, generate_signals
from core.utils.snapshot_builder import rebuild_market_snapshots
from core.constants import MIN_CANDLES, ALLOWED_TIMEFRAMES, SIGNAL_NA

from app.pipes.pipe_binance import run_pipe_binance


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
DATA_FILE = os.path.join(
    PROJECT_ROOT,
    "Domasno 1",
    "data",
    "cryptocurrency_binance_all.csv"
)


def home(request):
    return render(request, "home.html")


def learn(request):
    return render(request, "learn.html")


def about(request):
    return render(request, "about.html")


def contact(request):
    return render(request, "contact.html")


def run_pipeline(request):
    message = None
    if request.method == "POST":
        try:
            run_pipe_binance(1000, 3650)
            rebuild_market_snapshots()
            message = "Pipeline + snapshots finished successfully!"
        except Exception as e:
            message = f"Error: {e}"

    return render(request, "run_pipeline.html", {"message": message})


def analyze(request):
    snapshots = MarketSnapshot.objects.order_by("symbol")
    paginator = Paginator(snapshots, 50)
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)

    return render(request, "analyze.html", {"page_obj": page_obj})


def data_overview(request):
    if not os.path.exists(DATA_FILE):
        return HttpResponse("Please run the pipeline first.")

    symbols = set()
    with open(DATA_FILE, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sym = row.get("symbol")
            if sym and sym.endswith("USDT"):
                symbols.add(sym.strip())

    return render(request, "data_overview.html", {
        "symbols": sorted(symbols)
    })


def symbol_detail(request, symbol):
    timeframe = request.GET.get("tf", "daily")
    if timeframe not in ALLOWED_TIMEFRAMES:
        timeframe = "daily"

    qs = CryptoOHLCV.objects.filter(symbol=symbol).order_by("date")
    df = queryset_to_df(qs)

    if df.empty:
        return render(request, "symbol_detail.html", {
            "symbol": symbol,
            "timeframe": timeframe,
            "latest_signal": SIGNAL_NA,
            "rows": [],
            "has_enough_data": False,
            "min_required": MIN_CANDLES[timeframe],
            "actual_candles": 0,
        })

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df.sort_values("date")

    if timeframe != "daily":
        df = resample_timeframe(df, timeframe)

    min_required = MIN_CANDLES[timeframe]
    if len(df) < min_required:
        return render(request, "symbol_detail.html", {
            "symbol": symbol,
            "timeframe": timeframe,
            "latest_signal": SIGNAL_NA,
            "rows": [],
            "has_enough_data": False,
            "min_required": min_required,
            "actual_candles": len(df),
        })

    df = compute_indicators(df)
    df = generate_signals(df)

    df = df.tail(500)

    df["ts"] = df["date"].astype("int64") // 10**6

    rows = df[["ts", "close"]].to_dict("records")
    latest_signal = df.iloc[-1].get("signal", SIGNAL_NA)

    return render(request, "symbol_detail.html", {
        "symbol": symbol,
        "timeframe": timeframe,
        "latest_signal": latest_signal,
        "rows": rows,
        "has_enough_data": True,
        "min_required": min_required,
        "actual_candles": len(df),
    })
