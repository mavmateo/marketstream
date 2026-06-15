import json
import logging

from typing import Callable
from config.kafka_config import KafkaConfig

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable


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
            logger.info("="*75)
            self._running = True
            logger.info("Consuming clean crypto from kafka....")
            self.connect()
            self._listen()



    def connect(self) -> None:
            logger.info("="*75)
            logger.info("Connecting to kafka....")

            try:
                    self._consumer = KafkaConsumer(
                            bootstrap_servers = self.config.bootstrap_servers,
                            value_deserializer = lambda x: json.loads(x.decode('utf-8')),

                    )
                    logger.info("="*75)
                    logger.info("Kafka consumer initiated....")

            except NoBrokersAvailable as exc:
                    raise ConnectionError(
                             f"Could not connect to Kafka at {self.config.bootstrap_servers}. "
                "Is the broker running?"
                    ) from exc
            
            self._consumer.subscribe([self.topic])
            logger.info("="*75)
            logger.info("Subscribed to topic(s). Waiting for messages...")


    def _listen(self) -> None:
            try:
                for msg in self._consumer:
                        if not self._running:
                                break
                        
                        try:
                            logger.info("=" * 75)
                            logger.info("Received Message:")
                            logger.info("   Topic    :%s",  {msg.topic})
                            logger.info("   Partition:%s", {msg.partition})
                            logger.info("   Offset   :%s", {msg.offset})
                            logger.info("   Key      :%s", {msg.key})
                            logger.info("   Value    : %s", str(msg.value)[:200])  
                            logger.info("=" * 75)

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
    logger.info("="*75)
    received : list[dict] = []    

    def on_message(msg: dict) -> None: 
        logger.info("="*75)
        received.append(msg)
        logger.info(
        "[%s] %s O=%.2f H=%.2f L=%.2f C=%.2f vol=%.3f",
        msg['timestamp'], msg['symbol'],
        msg['open'], msg['high'],
        msg['low'],  msg['close'], msg['volume']
    )

    if len(received) >= 5:
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

                   
                    
                            
                     

                         
