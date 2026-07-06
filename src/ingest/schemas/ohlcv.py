from pydantic import BaseModel, field_validator
from datetime import datetime

class OHLCVTick(BaseModel):
    symbol: str
    market: str
    timestamp: str
    open:   float
    high: float
    low: float
    close: float
    price: float
    volume: float
    trades : int | None = None
    vwap : float | None = None
    is_bullish : bool | None = None
    price_range : float | None = None
    body_size : float | None = None
    upper_shadow: float | None = None
    lower_shadow: float | None = None


    @field_validator("high")
    @classmethod
    def high_must_be_highest(cls, v, info):
        if "low" in info.data and v < info.data["low"]:
            raise ValueError("high must be >= low")
        return v
