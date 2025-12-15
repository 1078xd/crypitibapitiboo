import pandas as pd
from core.constants import ALLOWED_TIMEFRAMES


_TIMEFRAME_TO_RULE = {
    "daily": None,
    "weekly": "W",
    "monthly": "ME",
}


def resample_timeframe(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if timeframe not in ALLOWED_TIMEFRAMES:
        raise ValueError(f"Invalid timeframe: {timeframe}")

    rule = _TIMEFRAME_TO_RULE[timeframe]

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df.sort_values("date")

    if rule is None:
        return df

    df = df.set_index("date")

    agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }

    agg = {k: v for k, v in agg.items() if k in df.columns}

    out = df.resample(rule).agg(agg)
    out = out.dropna(subset=["open", "high", "low", "close"])
    out = out.reset_index()

    return out
