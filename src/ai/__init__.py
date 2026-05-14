# ai/__init__.py

from src.logger.logger import get_config
from src.logger.logger import get_logger
from config.model_config import ModelConfig

logger = get_logger(__name__)
config = ModelConfig()

from ai.models.price_predictor import PricePredictor
from ai.models.prophet_model import ProphetModel
from ai.models.lstm_model import LSTMModel
from ai.signals.sentiment_analyzer import SentimentAnalyzer
from ai.signals.trend_detector import TrendDetector
from ai.signals.anomaly_detector import AnomalyDetector

__all__ = [
    "PricePredictor",
    "ProphetModel",
    "LSTMModel",
    "SentimentAnalyzer",
    "TrendDetector",
    "AnomalyDetector",
]