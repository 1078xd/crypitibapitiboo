from __future__ import annotations

from typing import Any, Dict
import pandas as pd

from app.indicators.base import SignalStrategy

SIGNAL_NA = "N/A"
SIGNAL_BUY = "BUY"
SIGNAL_SELL = "SELL"
SIGNAL_HOLD = "HOLD"


def any_missing(*vals) -> bool:
    """True if any value is NaN/None."""
    return any(pd.isna(v) for v in vals)


class RSISignal(SignalStrategy):
    @property
    def label(self) -> str:
        return "RSI (14)"

    def compute(self, latest: pd.Series, values: Dict[str, Any]) -> str:
        rsi = values.get("rsi")
        if pd.isna(rsi):
            return SIGNAL_NA
        if rsi < 30:
            return SIGNAL_BUY
        if rsi > 70:
            return SIGNAL_SELL
        return SIGNAL_HOLD


class MACDSignal(SignalStrategy):
    @property
    def label(self) -> str:
        return "MACD"

    def compute(self, latest: pd.Series, values: Dict[str, Any]) -> str:
        macd = values.get("macd")
        macd_signal = values.get("macd_signal")
        if any_missing(macd, macd_signal):
            return SIGNAL_NA
        if macd > macd_signal:
            return SIGNAL_BUY
        if macd < macd_signal:
            return SIGNAL_SELL
        return SIGNAL_HOLD


class StochasticSignal(SignalStrategy):
    @property
    def label(self) -> str:
        return "Stochastic Oscillator"

    def compute(self, latest: pd.Series, values: Dict[str, Any]) -> str:
        k = values.get("stoch_k")
        d = values.get("stoch_d")
        if any_missing(k, d):
            return SIGNAL_NA
        if k < 20 and d < 20:
            return SIGNAL_BUY
        if k > 80 and d > 80:
            return SIGNAL_SELL
        return SIGNAL_HOLD


class ADXSignal(SignalStrategy):
    @property
    def label(self) -> str:
        return "ADX (14)"

    def compute(self, latest: pd.Series, values: Dict[str, Any]) -> str:
        adx = values.get("adx")
        close = latest.get("close")
        sma20 = values.get("sma_20")
        if any_missing(adx, close, sma20):
            return SIGNAL_NA
        if adx < 20:
            return SIGNAL_HOLD
        if close > sma20:
            return SIGNAL_BUY
        if close < sma20:
            return SIGNAL_SELL
        return SIGNAL_HOLD


class CCISignal(SignalStrategy):
    @property
    def label(self) -> str:
        return "CCI (20)"

    def compute(self, latest: pd.Series, values: Dict[str, Any]) -> str:
        cci = values.get("cci")
        if pd.isna(cci):
            return SIGNAL_NA
        if cci < -100:
            return SIGNAL_BUY
        if cci > 100:
            return SIGNAL_SELL
        return SIGNAL_HOLD


class MASignal(SignalStrategy):
    """
    Reusable MA comparison strategy.
    Example: MASignal("SMA (20)", "sma_20")
    """
    def __init__(self, label: str, ma_key: str):
        self._label = label
        self._ma_key = ma_key

    @property
    def label(self) -> str:
        return self._label

    def compute(self, latest: pd.Series, values: Dict[str, Any]) -> str:
        price = latest.get("close")
        ma = values.get(self._ma_key)
        if any_missing(price, ma):
            return SIGNAL_NA
        if price > ma:
            return SIGNAL_BUY
        if price < ma:
            return SIGNAL_SELL
        return SIGNAL_HOLD


class BollingerSignal(SignalStrategy):
    @property
    def label(self) -> str:
        return "Bollinger Bands"

    def compute(self, latest: pd.Series, values: Dict[str, Any]) -> str:
        close = latest.get("close")
        lower = values.get("bb_lower")
        upper = values.get("bb_upper")
        if any_missing(close, lower, upper):
            return SIGNAL_NA
        if close < lower:
            return SIGNAL_BUY
        if close > upper:
            return SIGNAL_SELL
        return SIGNAL_HOLD


class VolumeSMASignal(SignalStrategy):
    @property
    def label(self) -> str:
        return "Volume SMA (20)"

    def compute(self, latest: pd.Series, values: Dict[str, Any]) -> str:
        volume = latest.get("volume")
        vol_sma = values.get("vol_sma_20")
        if any_missing(volume, vol_sma):
            return SIGNAL_NA
        if volume > vol_sma:
            return SIGNAL_BUY
        if volume < vol_sma:
            return SIGNAL_SELL
        return SIGNAL_HOLD
