import pandas as pd
from sqlalchemy import create_engine

DB_URL = "postgresql://postgres:postgres@localhost:5432/marketstream"
engine = create_engine(DB_URL)

def get_ohlcv(symbol: str, limit: int = 60) -> pd.DataFrame:
    query = """
        SELECT time, open, high, low, close, volume
        FROM ohlcv_ticks
        WHERE symbol = %(symbol)s
        ORDER BY time DESC
        LIMIT %(limit)s
    """
    df = pd.read_sql(query, engine, params={"symbol": symbol, "limit": limit })
    return df.sort_values("time")


def get_signals(symbol: str, limit: int=20) -> pd.DataFrame:
    query = """
        SELECT time, signal_type, direction, confidence, predicted_price, details
        FROM ai_signals
        WHERE symbol = %(symbol)s
        ORDER BY time DESC
        LIMIT %(limit)s
    """    
    return pd.read_sql(query, engine, params={"symbol": symbol, "limit": limit})


def get_all_symbols() -> list[str]:
    query = "SELECT DISTINCT symbol FROM ohlcv_ticks ORDER BY symbol"
    df = pd.read_sql(query, engine)
    return df["symbol"].tolist()


