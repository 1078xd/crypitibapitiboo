import pandas as pd
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.trend import MACD, ADXIndicator, CCIIndicator, SMAIndicator, EMAIndicator, WMAIndicator
from ta.volatility import BollingerBands

from core.constants import SIGNAL_NA, SIGNAL_BUY, SIGNAL_SELL, SIGNAL_HOLD


def _ensure_numeric(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = _ensure_numeric(df)
    df = df.dropna(subset=["high", "low", "close"])

    # Oscillators
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

    # Volume MA
    if "volume" in df.columns:
        df["vol_sma_20"] = SMAIndicator(close=df["volume"], window=20).sma_indicator()
    else:
        df["vol_sma_20"] = pd.NA

    return df


def _na_if_missing(*vals) -> bool:
    return any(pd.isna(v) for v in vals)


def _sig_rsi(rsi):
    if pd.isna(rsi):
        return SIGNAL_NA
    if rsi < 30:
        return SIGNAL_BUY
    if rsi > 70:
        return SIGNAL_SELL
    return SIGNAL_HOLD


def _sig_macd(macd, macd_signal):
    if _na_if_missing(macd, macd_signal):
        return SIGNAL_NA
    if macd > macd_signal:
        return SIGNAL_BUY
    if macd < macd_signal:
        return SIGNAL_SELL
    return SIGNAL_HOLD


def _sig_stoch(k, d):
    if _na_if_missing(k, d):
        return SIGNAL_NA
    if k < 20 and d < 20:
        return SIGNAL_BUY
    if k > 80 and d > 80:
        return SIGNAL_SELL
    return SIGNAL_HOLD


def _sig_adx(adx, close, sma20):
    # ADX strength filter + direction from close vs SMA20
    if _na_if_missing(adx, close, sma20):
        return SIGNAL_NA
    if adx < 20:
        return SIGNAL_HOLD
    if close > sma20:
        return SIGNAL_BUY
    if close < sma20:
        return SIGNAL_SELL
    return SIGNAL_HOLD


def _sig_cci(cci):
    if pd.isna(cci):
        return SIGNAL_NA
    if cci < -100:
        return SIGNAL_BUY
    if cci > 100:
        return SIGNAL_SELL
    return SIGNAL_HOLD


def _sig_ma(x, ma):
    if _na_if_missing(x, ma):
        return SIGNAL_NA
    if x > ma:
        return SIGNAL_BUY
    if x < ma:
        return SIGNAL_SELL
    return SIGNAL_HOLD


def _sig_bollinger(close, lower, upper):
    if _na_if_missing(close, lower, upper):
        return SIGNAL_NA
    if close < lower:
        return SIGNAL_BUY
    if close > upper:
        return SIGNAL_SELL
    return SIGNAL_HOLD


def _majority_vote(signals):
    valid = [s for s in signals if s in (SIGNAL_BUY, SIGNAL_SELL, SIGNAL_HOLD)]
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


def build_signals_snapshot(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {"values": {}, "signals": {}, "overall": SIGNAL_NA, "latest": {}}

    last = df.iloc[-1]

    latest = {
        "date": last.get("date"),
        "open": last.get("open"),
        "high": last.get("high"),
        "low": last.get("low"),
        "close": last.get("close"),
        "volume": last.get("volume"),
    }

    values = {
        # Oscillators
        "rsi": last.get("rsi"),
        "macd": last.get("macd"),
        "macd_signal": last.get("macd_signal"),
        "stoch_k": last.get("stoch_k"),
        "stoch_d": last.get("stoch_d"),
        "adx": last.get("adx"),
        "cci": last.get("cci"),

        # Moving averages group
        "sma_20": last.get("sma_20"),
        "ema_20": last.get("ema_20"),
        "wma_20": last.get("wma_20"),
        "bb_lower": last.get("bb_lower"),
        "bb_upper": last.get("bb_upper"),
        "vol_sma_20": last.get("vol_sma_20"),
    }

    close = latest["close"]
    volume = latest["volume"]

    signals = {
        # 5 oscillators
        "RSI (14)": _sig_rsi(values["rsi"]),
        "MACD": _sig_macd(values["macd"], values["macd_signal"]),
        "Stochastic Oscillator": _sig_stoch(values["stoch_k"], values["stoch_d"]),
        "ADX (14)": _sig_adx(values["adx"], close, values["sma_20"]),
        "CCI (20)": _sig_cci(values["cci"]),

        # 5 moving averages
        "SMA (20)": _sig_ma(close, values["sma_20"]),
        "EMA (20)": _sig_ma(close, values["ema_20"]),
        "WMA (20)": _sig_ma(close, values["wma_20"]),
        "Bollinger Bands": _sig_bollinger(close, values["bb_lower"], values["bb_upper"]),
        "Volume SMA (20)": _sig_ma(volume, values["vol_sma_20"]),
    }

    overall = _majority_vote(list(signals.values()))
    return {"latest": latest, "values": values, "signals": signals, "overall": overall}
