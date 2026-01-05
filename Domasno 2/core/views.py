import os
import csv
import pandas as pd

from django.shortcuts import render
from django.http import HttpResponse
from django.core.paginator import Paginator

from core.models import CryptoOHLCV, MarketSnapshot
from core.utils.queryset_to_df import queryset_to_df
from core.utils.timeframes import resample_timeframe
from core.indicators.indicators import compute_indicators, build_signals_snapshot
from core.utils.snapshot_builder import rebuild_market_snapshots
from core.constants import (
    MIN_CANDLES,
    ALLOWED_TIMEFRAMES,
    SIGNAL_NA,
    SIGNAL_BUY,
    SIGNAL_SELL,
    SIGNAL_HOLD,
)

from app.pipes.pipe_binance import run_pipe_binance


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
DATA_FILE = os.path.join(
    PROJECT_ROOT,
    "Domasno 1",
    "data",
    "cryptocurrency_binance_all.csv"
)


def majority_vote_3(daily: str, weekly: str, monthly: str) -> str:
    """
    Combine daily/weekly/monthly signals into one by majority vote.
    Ignores N/A values. If tie, returns HOLD.
    """
    vals = [daily, weekly, monthly]
    valid = [v for v in vals if v in (SIGNAL_BUY, SIGNAL_SELL, SIGNAL_HOLD)]

    if not valid:
        return SIGNAL_NA

    b = valid.count(SIGNAL_BUY)
    s = valid.count(SIGNAL_SELL)
    h = valid.count(SIGNAL_HOLD)

    if b > s and b > h:
        return SIGNAL_BUY
    if s > b and s > h:
        return SIGNAL_SELL
    return SIGNAL_HOLD


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
    """
    Analyze list page with:
    - sorting (A–Z / Z–A)
    - signal filtering (BUY / HOLD / SELL)
    - search by symbol
    """
    sort = request.GET.get("sort", "asc")              # asc | desc
    signal_filter = request.GET.get("signal", "all")   # buy | sell | hold | all
    search = request.GET.get("search", "").strip()     # text search

    snapshots = list(MarketSnapshot.objects.all())

    # Attach combined signal (daily + weekly + monthly)
    for r in snapshots:
        r.combined_signal = majority_vote_3(
            r.daily_signal, r.weekly_signal, r.monthly_signal
        )

    # SEARCH (symbol contains text)
    if search:
        snapshots = [
            r for r in snapshots
            if search.lower() in r.symbol.lower()
        ]

    # FILTER by signal
    if signal_filter in ("buy", "sell", "hold"):
        snapshots = [
            r for r in snapshots
            if r.combined_signal.lower() == signal_filter
        ]

    # SORT by symbol
    snapshots.sort(
        key=lambda x: x.symbol,
        reverse=(sort == "desc")
    )

    paginator = Paginator(snapshots, 50)
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)

    context = {
        "page_obj": page_obj,
        "sort": sort,
        "signal_filter": signal_filter,
        "search": search,
    }

    return render(request, "analyze.html", context)


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

    return render(request, "data_overview.html", {"symbols": sorted(symbols)})


def symbol_detail(request, symbol):
    timeframe = request.GET.get("tf", "daily")
    if timeframe not in ALLOWED_TIMEFRAMES:
        timeframe = "daily"

    qs = CryptoOHLCV.objects.filter(symbol=symbol).order_by("date")
    df = queryset_to_df(qs)

    # Default context so template never crashes
    base_ctx = {
        "symbol": symbol,
        "timeframe": timeframe,
        "overall_signal": SIGNAL_NA,
        "latest": {},
        "signals": {},
        "values": {},
        "has_enough_data": False,
        "min_required": MIN_CANDLES[timeframe],
        "actual_candles": 0,
    }

    if df.empty:
        return render(request, "symbol_detail.html", base_ctx)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df.sort_values("date")

    if timeframe != "daily":
        df = resample_timeframe(df, timeframe)

    min_required = MIN_CANDLES[timeframe]
    if len(df) < min_required:
        base_ctx["min_required"] = min_required
        base_ctx["actual_candles"] = len(df)
        return render(request, "symbol_detail.html", base_ctx)

    df = compute_indicators(df)
    snapshot = build_signals_snapshot(df)

    ctx = {
        **base_ctx,
        "has_enough_data": True,
        "min_required": min_required,
        "actual_candles": len(df),
        "overall_signal": snapshot["overall"],
        "latest": snapshot["latest"],
        "signals": snapshot["signals"],
        "values": snapshot["values"],
    }

    return render(request, "symbol_detail.html", ctx)
