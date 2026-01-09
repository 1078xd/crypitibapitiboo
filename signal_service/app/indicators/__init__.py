from app.indicators.service import SignalEngine
from app.indicators.strategies import (
    RSISignal,
    MACDSignal,
    StochasticSignal,
    ADXSignal,
    CCISignal,
    MASignal,
    BollingerSignal,
    VolumeSMASignal,
)

DEFAULT_SIGNAL_ENGINE = SignalEngine(strategies=[
    RSISignal(),
    MACDSignal(),
    StochasticSignal(),
    ADXSignal(),
    CCISignal(),
    MASignal("SMA (20)", "sma_20"),
    MASignal("EMA (20)", "ema_20"),
    MASignal("WMA (20)", "wma_20"),
    BollingerSignal(),
    VolumeSMASignal(),
])
