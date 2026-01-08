import pandas as pd
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import MACD, ADXIndicator, CCIIndicator, SMAIndicator, EMAIndicator, WMAIndicator
from ta.volatility import BollingerBands

SIGNAL_NA = "N/A"
SIGNAL_BUY = "BUY"
SIGNAL_SELL = "SELL"
SIGNAL_HOLD = "HOLD"


def _ensure_numeric(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = _ensure_numeric(df)
    df = df.dropna(subset=["high", "low", "close"])

    df["rsi"] = RSIIndicator(close=df["close"], window=14).rsi()

    macd = MACD(close=df["close"])
    df["macd"] = macd.macd()
    df["macd_signal"] = macd.macd_signal()
    df["macd_hist"] = macd.macd_diff()

    stoch = StochasticOscillator(high=df["high"], low=df["low"], close=df["close"])
    df["stoch_k"] = stoch.stoch()
    df["stoch_d"] = stoch.stoch_signal()

    adx = ADXIndicator(high=df["high"], low=df["low"], close=df["close"], window=14)
    df["adx"] = adx.adx()

    df["cci"] = CCIIndicator(high=df["high"], low=df["low"], close=df["close"], window=20).cci()

    df["sma_20"] = SMAIndicator(close=df["close"], window=20).sma_indicator()
    df["ema_20"] = EMAIndicator(close=df["close"], window=20).ema_indicator()
    df["wma_20"] = WMAIndicator(close=df["close"], window=20).wma()

    bb = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["bb_upper"] = bb.bollinger_hband()
    df["bb_middle"] = bb.bollinger_mavg()
    df["bb_lower"] = bb.bollinger_lband()

    if "volume" in df.columns:
        df["vol_sma_20"] = SMAIndicator(close=df["volume"], window=20).sma_indicator()
    else:
        df["vol_sma_20"] = pd.NA

    return df

