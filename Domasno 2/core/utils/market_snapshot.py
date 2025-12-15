import pandas as pd

from core.indicators.indicators import compute_indicators, generate_signals
from core.utils.timeframes import resample_timeframe

def compute_signal_for_timeframe(df, timeframe):
    if timeframe != "daily":
        df = resample_timeframe(df, timeframe)

    min_required = MIN_CANDLES.get(timeframe, 30)
    
    if len(df) < min_required:
        return "N/A"

    try:
        df = compute_indicators(df)
        df = generate_signals(df)
    except Exception:
        return "N/A"

    return df.iloc[-1]["signal"]


def build_multitimeframe_snapshot(df, symbol):
    return {
        "symbol": symbol,
        "daily_signal": compute_signal_for_timeframe(df, "daily"),
        "weekly_signal": compute_signal_for_timeframe(df, "weekly"),
        "monthly_signal": compute_signal_for_timeframe(df, "monthly"),
    }
