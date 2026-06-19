
from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .trend_detector     import detect as trend_detect
    from .anomaly_detector   import AnomalyDetector
    from .sentiment_analyzer import SentimentAnalyzer


def __getattr__(name: str):
    _map = {
        "detect":            (".trend_detector",     "detect"),
        "AnomalyDetector":   (".anomaly_detector",    "AnomalyDetector"),
        "SentimentAnalyzer": (".sentiment_analyzer",  "SentimentAnalyzer"),
    }
    if name in _map:
        import importlib
        module_path, attr_name = _map[name]
        module = importlib.import_module(module_path, package=__name__)
        return getattr(module, attr_name)
    raise AttributeError(f"module 'ai.signals' has no attribute {name!r}")


__all__ = ["detect", "AnomalyDetector", "SentimentAnalyzer"]