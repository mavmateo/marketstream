# src/ingest/__init__.py

from __future__ import annotations
from typing import TYPE_CHECKING

# Only imported when a type-checker runs — zero cost at runtime
if TYPE_CHECKING:
    from .producers.stock_producer  import StockProducer
    from .producers.crypto_producer import CryptoProducer
    from .sources.alpaca_ws         import AlpacaWebSocket
    from .sources.binance_ws        import BinanceWebSocket
    from .sources.polygon_rest      import PolygonREST
    from .schemas.ohlcv_schema      import OHLCVSchema


def __getattr__(name: str):
    
    _map = {
        "StockProducer":   (".producers.stock_producer",  "StockProducer"),
        "CryptoProducer":  (".producers.crypto_producer", "CryptoProducer"),
        "AlpacaWebSocket": (".sources.alpaca_ws",         "AlpacaWebSocket"),
        "BinanceWebSocket":(".sources.binance_ws",        "BinanceWebSocket"),
        "PolygonREST":     (".sources.polygon_rest",      "PolygonREST"),
        "OHLCVSchema":     (".schemas.ohlcv_schema",      "OHLCVSchema"),
    }
    if name in _map:
        module_path, class_name = _map[name]
        import importlib
        module = importlib.import_module(module_path, package=__name__)
        return getattr(module, class_name)
    raise AttributeError(f"module 'ingest' has no attribute {name!r}")


__all__ = [
    "StockProducer", "CryptoProducer",
    "AlpacaWebSocket", "BinanceWebSocket",
    "PolygonREST", "OHLCVSchema",
]