from django.db import transaction
import pandas as pd
import requests

from core.models import CryptoOHLCV, MarketSnapshot
from core.utils.queryset_to_df import queryset_to_df
from core.utils.timeframes import resample_timeframe
from core.constants import MIN_CANDLES, SIGNAL_NA

SIGNALS_URL = "http://127.0.0.1:8001/signals" 


def compute_signal_for_timeframe(df: pd.DataFrame, timeframe: str) -> str:
    # Resample if needed
    if timeframe != "daily":
        df = resample_timeframe(df, timeframe)
    else:
        df = df.copy()

    min_required = MIN_CANDLES.get(timeframe, 120)
    if len(df) < min_required:
        return SIGNAL_NA

    df_send = df.tail(500).copy()

    candles = []
    for _, row in df_send.iterrows():
        date_val = None
        if "date" in df_send.columns and pd.notna(row.get("date")):
            date_val = pd.to_datetime(row["date"], errors="coerce")
            date_val = date_val.isoformat() if pd.notna(date_val) else None

        candles.append({
            "date": date_val,
            "open": float(row["open"]) if pd.notna(row.get("open")) else 0.0,
            "high": float(row["high"]) if pd.notna(row.get("high")) else 0.0,
            "low": float(row["low"]) if pd.notna(row.get("low")) else 0.0,
            "close": float(row["close"]) if pd.notna(row.get("close")) else 0.0,
            "volume": float(row["volume"]) if "volume" in df_send.columns and pd.notna(row.get("volume")) else None,
        })

    try:
        resp = requests.post(
            SIGNALS_URL,
            json={"timeframe": timeframe, "candles": candles},
            timeout=20
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("overall", SIGNAL_NA)
    except Exception:
        return SIGNAL_NA


@transaction.atomic
def rebuild_market_snapshots():
    """
    Rebuild snapshot table safely and atomically.
    Uses the Signals microservice for daily/weekly/monthly overall signals.
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

        if len(df) < MIN_CANDLES["daily"]:
            continue

        latest = df.iloc[-1]
        price = latest.get("close")
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
