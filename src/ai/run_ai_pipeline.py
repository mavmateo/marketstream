import logging
import threading
import psycopg2
import json

from psycopg2 import OperationalError

from config.kafka_config import KafkaConfig
from ai.signals.trend_detector import detect
from ai.models.price_predictor import PricePredictor
from ai.signals.anomaly_detector import AnomalyDetector
from ai.consumers.clean_crypto_consumer import CryptoConsumer
from ai.consumers.clean_stocks_consumer import StockConsumer
from utils.db import get_db_cursor


logger = logging.getLogger(__name__)


def _write_signals(signals: list[dict]) -> None:
    logger.info("="*85)
    logger.info("Writing signals to timescaledb ....")
    logger.info("="*85)

    with get_db_cursor() as cursor:

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

      

def _build_on_tick(trend_fn, anomaly_detector: AnomalyDetector, price_predictor: PricePredictor):
    
    def on_tick(tick : dict) -> None:
        try:
            trend_signal = trend_fn(tick)
            anomaly_signal = anomaly_detector.detect(tick)
            prediction_signal = price_predictor.predict(tick)    
            _write_signals([trend_signal,anomaly_signal,prediction_signal])
            logger.info("Signals successfully written to TimescaleDB....")

        except Exception as e:
            logger.error("on_tick failed for %s: %s", 
                         tick.get("symbol"), e, exc_info=True)

    return on_tick    



def _run() -> None:
    config = KafkaConfig()
    anomaly = AnomalyDetector()
    predictor = PricePredictor()

    on_tick = _build_on_tick(detect, anomaly, predictor)


    stock_consumer = StockConsumer(
        topic= config.CLEAN_STOCKS_TOPIC,
        callback = on_tick
    )

    crypto_consumer = CryptoConsumer(
        topic= config.CLEAN_CRYPTO_TOPIC,
        callback = on_tick
    )

    stock_thread = threading.Thread(target = stock_consumer.run, daemon = True)
    crypto_thread = threading.Thread(target= crypto_consumer.run, daemon=True)

    try:
        
        stock_thread.start()
        crypto_thread.start()

        stock_thread.join()
        crypto_thread.join()

    except KeyboardInterrupt:  
        logger.info("Shutdown requested - stopping consumers.")
        stock_consumer.stop()
        crypto_consumer.stop()
    except Exception as e:
        logger.error("AI pipeline failed: %s", e, exc_info=True)    
         





if __name__ == "__main__":
    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    _run()

