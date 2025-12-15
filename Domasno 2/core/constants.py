MIN_CANDLES = {
    "daily": 120,
    "weekly": 60,
    "monthly": 48,
}

ALLOWED_TIMEFRAMES = set(MIN_CANDLES.keys())

SIGNAL_NA = "N/A"
SIGNAL_BUY = "BUY"
SIGNAL_SELL = "SELL"
SIGNAL_HOLD = "HOLD"