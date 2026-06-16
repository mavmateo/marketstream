
from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .consumers.clean_stocks_consumer import StockConsumer
    from .consumers.clean_crypto_consumer import CryptoConsumer
    from .models.price_predictor         import PricePredictor
    from .models.prophet_model           import ProphetModel
    from .models.lstm_model              import LSTMModel
    from .signals.sentiment_analyzer     import SentimentAnalyzer
    from .signals.trend_detector         import TrendDetector
    from .signals.anomaly_detector       import AnomalyDetector


def __getattr__(name: str):
    _map = {
        "StockConsumer":     (".consumers.clean_stocks_consumer", "StockConsumer"),
        "CryptoConsumer":    (".consumers.clean_crypto_consumer", "CryptoConsumer"),
        "PricePredictor":    (".models.price_predictor",          "PricePredictor"),
        "ProphetModel":      (".models.prophet_model",            "ProphetModel"),
        "LSTMModel":         (".models.lstm_model",               "LSTMModel"),
        "SentimentAnalyzer": (".signals.sentiment_analyzer",      "SentimentAnalyzer"),
        "TrendDetector":     (".signals.trend_detector",          "TrendDetector"),
        "AnomalyDetector":   (".signals.anomaly_detector",        "AnomalyDetector"),
    }
    if name in _map:
        import importlib
        module_path, class_name = _map[name]
        module = importlib.import_module(module_path, package=__name__)
        return getattr(module, class_name)
    raise AttributeError(f"module 'ai' has no attribute {name!r}")


__all__ = [
    "StockConsumer", "CryptoConsumer",
    "PricePredictor", "ProphetModel", "LSTMModel",
    "SentimentAnalyzer", "TrendDetector", "AnomalyDetector",
]