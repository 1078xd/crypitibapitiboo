from __future__ import annotations

from typing import Any, Dict, List
import pandas as pd

from app.indicators.base import SignalStrategy

SIGNAL_NA = "N/A"
SIGNAL_BUY = "BUY"
SIGNAL_SELL = "SELL"
SIGNAL_HOLD = "HOLD"

_VALID = {SIGNAL_BUY, SIGNAL_SELL, SIGNAL_HOLD}


def majority_vote(signals: List[str]) -> str:
    """Pick the majority among BUY/SELL/HOLD. If tie -> HOLD. If none -> N/A."""
    valid = [s for s in signals if s in _VALID]
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


class SignalEngine:
    """Runs registered SignalStrategy objects to produce signals + overall vote."""
    def __init__(self, strategies: List[SignalStrategy]):
        self._strategies = strategies

    def build_snapshot(self, df: pd.DataFrame) -> Dict[str, Any]:
        if df is None or df.empty:
            return {"values": {}, "signals": {}, "overall": SIGNAL_NA, "latest": {}}

        last = df.iloc[-1]

        latest = {
            "date": str(last.get("date")) if "date" in df.columns else None,
            "open": last.get("open"),
            "high": last.get("high"),
            "low": last.get("low"),
            "close": last.get("close"),
            "volume": last.get("volume"),
        }

        # Values used by strategies (same keys as your current implementation)
        values = {
            "rsi": last.get("rsi"),
            "macd": last.get("macd"),
            "macd_signal": last.get("macd_signal"),
            "stoch_k": last.get("stoch_k"),
            "stoch_d": last.get("stoch_d"),
            "adx": last.get("adx"),
            "cci": last.get("cci"),
            "sma_20": last.get("sma_20"),
            "ema_20": last.get("ema_20"),
            "wma_20": last.get("wma_20"),
            "bb_lower": last.get("bb_lower"),
            "bb_upper": last.get("bb_upper"),
            "vol_sma_20": last.get("vol_sma_20"),
        }

        signals: Dict[str, str] = {
            strategy.label: strategy.compute(last, values)
            for strategy in self._strategies
        }

        overall = majority_vote(list(signals.values()))
        return {"latest": latest, "values": values, "signals": signals, "overall": overall}
