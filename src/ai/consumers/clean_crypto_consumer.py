import json
import logging

from typing import Callable
from config.kafka_config import KafkaConfig

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

from ai.signals.trend_detector import detect
from ai.signals.anomaly_detector import AnomalyDetector
from ai.models.price_predictor import PricePredictor





logger = logging.getLogger(__name__)


class CryptoConsumer:

    def __init__(self,
                 topic : str,
                 callback : Callable[[dict], None], 
                 config : KafkaConfig | None = None

                 ) -> None:
                 self.topic = topic
                 self.callback = callback
                 self.config = config or KafkaConfig()
                 self._consumer : KafkaConfig | None = None
                 self._running = False


    def run(self) -> None:
            logger.info("="*85)
            self._running = True
            logger.info("Consuming clean crypto from kafka....")
            self.connect()
            self._listen()



    def connect(self) -> None:
            logger.info("="*85)
            logger.info("Connecting to kafka....")

            try:
                    self._consumer = KafkaConsumer(
                            bootstrap_servers = self.config.bootstrap_servers,
                            value_deserializer = lambda x: json.loads(x.decode('utf-8')),

                    )
                    logger.info("="*85)
                    logger.info("Kafka consumer initiated....")

            except NoBrokersAvailable as exc:
                    raise ConnectionError(
                             f"Could not connect to Kafka at {self.config.bootstrap_servers}. "
                "Is the broker running?"
                    ) from exc
            
            self._consumer.subscribe([self.topic])
            logger.info("="*85)
            logger.info("Subscribed to topic(s). Waiting for messages...")


    def _listen(self) -> None:
            try:
                for msg in self._consumer:
                        if not self._running:
                                break
                        
                        try:
                            logger.info("=" * 85)
                            logger.info("Received Message:")
                            logger.info("   Topic    :%s",  {msg.topic})
                            logger.info("   Partition:%s", {msg.partition})
                            logger.info("   Offset   :%s", {msg.offset})
                            logger.info("   Key      :%s", {msg.key})
                            logger.info("   Value    : %s", str(msg.value)[:200])  
                            logger.info("=" * 85)

                            logger.debug("Topic: %s Symbol: %s Offset: %d", 
                                        msg.topic, msg.value.get("symbol"), msg.offset)
                            
                            self.callback(msg.value)   
                            
                        except Exception as e:
                         logger.info("Failed to process message: %s", e, exc_info=True)
                         continue
            except KeyboardInterrupt:
             logger.info("Stopping consumer...")    


    def stop(self) -> None:
          logger.info("="*75)
          logger.info("Stop requested on kafka consuming...") 
          self._running = False
          self._consumer.close()


def _smoke_test() -> None:
    logger.info("="*85)
    received : list[dict] = []    

    anomal_detector = AnomalyDetector()
    price_predictor = PricePredictor()

    def on_message(tick: dict) -> None: 
        logger.info("="*85)
        received.append(tick)

        trend_signal = detect(tick)
        anomaly_signal = anomal_detector.detect(tick)
        predicted_price = price_predictor.predict(tick)
        
        logger.info(
            "[%s] %-10s  trend=%-15s anomaly=%-8s confidence=%.3f predicted_price=%.3f",
            tick["timestamp"], tick["symbol"],
            trend_signal["direction"], anomaly_signal["direction"],anomaly_signal["confidence"],predicted_price["predicted_price"]
        )

    if len(received) >= 50:
     consumer_instance.stop() 

    consumer_instance = CryptoConsumer(
        topic = KafkaConfig().CLEAN_CRYPTO_TOPIC,
        callback = on_message
    )
    consumer_instance.run()
    print(f"Smoke test complete. Received {len(received)} messages." )


if __name__ == "__main__":
    logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s",

    )
    _smoke_test()

                   
                    
                            
                     

                         
