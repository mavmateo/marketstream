import json 
import logging


from typing import Callable
from config.kafka_config import KafkaConfig

from kafka import KafkaConsumer
from kafka.errors import KafkaError,  NoBrokersAvailable

logger = logging.getLogger(__name__)



class StockConsumer : 
    def __init__(
            self,
            topic : str,
            callback:  Callable[[dict], None],
            config  : KafkaConfig | None = None,
        
        ) -> None:
        self.topic = topic
        self.callback = callback
        self.config = config or KafkaConfig()
        self._consumer : KafkaConsumer | None = None
        self._running = False
       



    def run(self) -> None:    
        logger.info("="*75)
        self._running   = True
        logger.info("Consuming clean stocks from kafka ....")
        self.connect()
        self._listen()


    def connect(self) -> None:
        logger.info("="*75) 
        logger.info("Connecting to Kafka....")
        try:
            self._consumer = KafkaConsumer(
                bootstrap_servers = self.config.bootstrap_servers,
                value_deserializer = lambda x: json.loads(x.decode('utf-8')),
                
            )
            logger.info("Kafka consumer initiated...")

        except NoBrokersAvailable as exc:
            raise ConnectionError(
                f"Could not connect to Kafka at {self.config.bootstrap_servers}. "
                "Is the broker running?"
            ) from exc 
        
        self._consumer.subscribe([self.topic])
        logger.info("Subscribed to topic(s). Waiting for messages...")    


    def _listen(self) -> None:
                   
        try:
            for msg in self._consumer:
                if not self._running: 
                    break
                                       
                
                try:
                    
                    logger.info("=" * 75)
                    logger.info(f"Received Message:")
                    logger.info(f"   Topic    : {msg.topic}")
                    logger.info(f"   Partition: {msg.partition}")
                    logger.info(f"   Offset   : {msg.offset}")
                    logger.info(f"   Key      : {msg.key}")
                    logger.info("   Value    : %s", str(msg.value)[:200])  
                    logger.info("=" * 75)
                    
                    

                except Exception as e:
                    logger.info(f"Failed to decode message: {e}")

                self.callback(msg.value)     
             

        except KeyboardInterrupt:
           logger.info("Stopping consumer...")

        


    def stop(self) -> None:   
        logger.info("="*75)
        logger.info("Stop requested on Kafka consuming.")
        self._running   = False
        self._consumer.close() 

  
    

def _smoke_test() -> None:
    logger.info("="*75)
    received : list[dict] = []

    def on_message(msg: dict) -> None:
        logger.info("="*75)
        received.append(msg)
        logger.info(
            f"[{msg['timestamp']}] {msg['symbol']:10s} "
            f"O={msg['open']:.2f} H={msg['high']:.2f} "
            f"L={msg['low']:.2f}  C={msg['close']:.2f} "
            f"vol={msg['volume']:.3f}"
        )


    consumer_instance = StockConsumer(

    topic= KafkaConfig().CLEAN_STOCKS_TOPIC ,    
    callback = on_message
    )
    consumer_instance.run()

    if len(received) >= 5:
      consumer_instance.stop()

   


if __name__ == "__main__":
    logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s",

    )
    _smoke_test()
         