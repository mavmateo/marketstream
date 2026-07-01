import logging
import threading
import psycopg2
import json

from typing import Callable

from config.kafka_config import KafkaConfig


from ai.signals.trend_detector import detect


from ai.models.price_predictor import PricePredictor
from ai.models.prophet_model import ProphetModel
from ai.models.lstm_model import LSTMModel
from ai.signals.sentiment_analyzer import SentimentAnalyzer
from ai.signals.trend_detector import TrendDetector
from ai.signals.anomaly_detector import AnomalyDetector

from ai.consumers.clean_crypto_consumer import CryptoConsumer
from ai.consumers.clean_stocks_consumer import StockConsumer


logger = logging.getLogger(__name__)


def _write_signals(signals: list[dict]) -> None:
    logger.info("="*85)
    logger.info("Writing signals to timescaledb ....")
    
    conn = psycopg2.connect(
        host ="localhost", port = 5432,
        dbname = "marketstream", user = "postgres",
        password = "postgres"

    )

    cursor = conn.cursor()

    for signal in signals:
        if signal["direction"] in ("INSUFFICIENT_DATA",):
            continue


        cursor.execute("""

                   INSERT INTO ai_signals
                (time, symbol, market, signal_type, direction,
                 confidence, predicted_price, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            signal["time"],
            signal["symbol"],
            signal["market"],
            signal["signal_type"],
            signal["direction"],
            signal["confidence"],
            signal.get("predicted_price"),
            json.dumps(signal["details"]),
        ))

        conn.commit()
        cursor.close()
        conn.close()





def _build_on_tick(trend_detector, anomaly_detector, price_predictor) -> Callable:
    
    def on_tick(tick : dict) -> None:

        trend_signal = trend_detector(tick)
        anomaly_signal = anomaly_detector(tick)
        prediction_signal = price_predictor(tick)
    
        _write_signals(trend_signal,anomaly_signal,prediction_signal)

    return on_tick    


def _run() -> None:
    logger.info("="*85)

    stock_thread = threading.thread(target = stock_consumer.run, daemon = True)
    crypto_thread = threading.Thread(target=crypto_consumer.run, daemon=True)

    stock_thread.start()
    crypto_thread.start()

    stock_thread.join()
    crypto_thread.join()

    on_tick = _build_on_tick(detect, anomaly_detector, price_predictor)


    stock_consumer = StockConsumer(
        topic= config.CLEAN_STOCKS_TOPIC,
        callback = on_tick
    )

    crypto_consumer = CryptoConsumer(
        topic= config.CLEAN_CRYPTO_TOPIC,
        callback = on_tick
    )





















    if __name__ == "__main__":
        logging.basicConfig(
            level  = logging.INFO,
            format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        )
        _run()

