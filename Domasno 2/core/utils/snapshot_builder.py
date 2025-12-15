from django.db import transaction
import pandas as pd

from core.models import CryptoOHLCV, MarketSnapshot
from core.utils.queryset_to_df import queryset_to_df
from core.utils.timeframes import resample_timeframe
from core.indicators.indicators import compute_indicators, generate_signals
from core.constants import MIN_CANDLES, SIGNAL_NA


def compute_signal_for_timeframe(df: pd.DataFrame, timeframe: str) -> str:
    # Resample if needed
    if timeframe != "daily":
        df = resample_timeframe(df, timeframe)
    else:
        df = df.copy()

    # Hard length check
    min_required = MIN_CANDLES.get(timeframe, 120)
    if len(df) < min_required:
        return SIGNAL_NA

    try:
        df = compute_indicators(df)
        df = generate_signals(df)
    except Exception:
        return SIGNAL_NA

    if "signal" not in df.columns:
        return SIGNAL_NA

    last_signal = df.iloc[-1]["signal"]
    return last_signal if isinstance(last_signal, str) else SIGNAL_NA


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

        # If you store daily candles, "24h volume" isn't df.tail(24) (that's 24 days).
        # For integrity, we store "recent volume" as last daily candle volume.
        # If you truly want 24h volume, fetch hourly candles from Binance pipeline.
        volume_24h = latest.get("volume")

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