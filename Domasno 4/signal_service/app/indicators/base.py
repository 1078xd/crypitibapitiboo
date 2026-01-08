from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict
import pandas as pd


class SignalStrategy(ABC):
    """
    Strategy interface for generating a BUY/SELL/HOLD/N/A signal from the latest candle
    and computed indicator values.
    """

    @property
    @abstractmethod
    def label(self) -> str:
        """Human-readable name shown in the API response."""
        raise NotImplementedError

    @abstractmethod
    def compute(self, latest: pd.Series, values: Dict[str, Any]) -> str:
        """Return one of: BUY / SELL / HOLD / N/A"""
        raise NotImplementedError
