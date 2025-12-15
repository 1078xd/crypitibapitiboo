import pandas as pd
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import MACD, ADXIndicator, CCIIndicator, SMAIndicator, EMAIndicator, WMAIndicator
from ta.volatility import BollingerBands

from core.constants import SIGNAL_NA, SIGNAL_BUY, SIGNAL_SELL, SIGNAL_HOLD


def _ensure_numeric(df: pd.DataFrame) -> pd.DataFrame:
    # Convert common OHLCV columns to numeric safely
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()
    df = _ensure_numeric(df)

    # Drop rows with missing OHLC (volume may be missing sometimes; we coerce it)
    df = df.dropna(subset=["high", "low", "close"])

    # Oscillators / trend measures
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

    # Moving averages
    df["sma_20"] = SMAIndicator(close=df["close"], window=20).sma_indicator()
    df["ema_20"] = EMAIndicator(close=df["close"], window=20).ema_indicator()
    df["wma_20"] = WMAIndicator(close=df["close"], window=20).wma()

    # Bollinger Bands
    bb = BollingerBands(close=df["close"], window=20, window_dev=2)
    df["bb_upper"] = bb.bollinger_hband()
    df["bb_middle"] = bb.bollinger_mavg()
    df["bb_lower"] = bb.bollinger_lband()

    # Volume SMA
    if "volume" in df.columns:
        df["vol_sma_20"] = SMAIndicator(close=df["volume"], window=20).sma_indicator()

    return df


def generate_signals(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()
    df["signal"] = SIGNAL_NA

    required_cols = ["rsi", "macd", "macd_signal", "sma_20", "close"]
    if any(col not in df.columns for col in required_cols):
        return df

    last = df.iloc[-1]
    if pd.isna(last[required_cols]).any():
        return df

    df["signal"] = SIGNAL_HOLD

    buy_condition = (
        (df["rsi"] < 30) &
        (df["macd"] > df["macd_signal"]) &
        (df["close"] > df["sma_20"])
    )

    sell_condition = (
        (df["rsi"] > 70) &
        (df["macd"] < df["macd_signal"]) &
        (df["close"] < df["sma_20"])
    )

    df.loc[buy_condition, "signal"] = SIGNAL_BUY
    df.loc[sell_condition, "signal"] = SIGNAL_SELL

    return df
