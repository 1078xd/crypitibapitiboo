from django.db import transaction
import pandas as pd

from core.models import CryptoOHLCV, MarketSnapshot
from core.utils.queryset_to_df import queryset_to_df
from core.utils.timeframes import resample_timeframe
from core.indicators.indicators import compute_indicators, build_signals_snapshot
from core.constants import MIN_CANDLES, SIGNAL_NA


def compute_signal_for_timeframe(df: pd.DataFrame, timeframe: str) -> str:
    """
    Returns overall BUY/SELL/HOLD/N/A for a given timeframe using the
    10-indicator majority vote from build_signals_snapshot().
    """
    # Resample if needed
    if timeframe != "daily":
        df_tf = resample_timeframe(df.copy(), timeframe)
    else:
        df_tf = df.copy()

    # Hard length check (before indicators)
    min_required = MIN_CANDLES.get(timeframe, MIN_CANDLES["daily"])
    if len(df_tf) < min_required:
        return SIGNAL_NA

    try:
        df_tf = compute_indicators(df_tf)
        snap = build_signals_snapshot(df_tf)
        return snap.get("overall", SIGNAL_NA) or SIGNAL_NA
    except Exception:
        return SIGNAL_NA


@transaction.atomic
def rebuild_market_snapshots():
    """
    Rebuild snapshot table safely and atomically.
    If it fails, the DB won't be left half-updated.
    """
    MarketSnapshot.objects.all().delete()

    symbols = (
        CryptoOHLCV.objects
        .values_list("symbol", flat=True)
        .distinct()
    )

    created = 0

    for symbol in symbols:
        qs = CryptoOHLCV.objects.filter(symbol=symbol).order_by("date")
        df = queryset_to_df(qs)

        # Must have at least daily minimum, otherwise don't create snapshot
        if len(df) < MIN_CANDLES["daily"]:
            continue

        latest = df.iloc[-1]
        price = latest.get("close")
        volume_24h = latest.get("volume")  # last candle volume (daily)

        daily_signal = compute_signal_for_timeframe(df, "daily")
        weekly_signal = compute_signal_for_timeframe(df, "weekly")
        monthly_signal = compute_signal_for_timeframe(df, "monthly")

        MarketSnapshot.objects.create(
            symbol=symbol,
            price=price,
            volume_24h=volume_24h,
            daily_signal=daily_signal,
            weekly_signal=weekly_signal,
            monthly_signal=monthly_signal,
        )

        created += 1

    print(f"Snapshots created: {created}")
