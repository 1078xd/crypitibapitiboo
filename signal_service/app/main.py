from fastapi import FastAPI
from pydantic import BaseModel, Field
from typing import List, Optional
import pandas as pd

from app.indicators import DEFAULT_SIGNAL_ENGINE
from app.indicator_calculator import compute_indicators



app = FastAPI(title="Signals Service", version="1.0.0")


class Candle(BaseModel):
    date: Optional[str] = None
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = None


class SignalsRequest(BaseModel):
    timeframe: str = Field(default="daily")
    candles: List[Candle]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/signals")
def signals(req: SignalsRequest):
    df = pd.DataFrame([c.model_dump() for c in req.candles])

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df = compute_indicators(df)
    snap = DEFAULT_SIGNAL_ENGINE.build_snapshot(df)

    snap["timeframe"] = req.timeframe
    snap["candles_used"] = len(df)
    return snap
