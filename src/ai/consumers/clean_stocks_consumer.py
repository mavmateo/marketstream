import json 
import logging
import time


from datetime import datetime, timedelta, timezone
from config.api_config import AlpacaConfig
from config.kafka_config import KafkaConfig

from kafka import KafkaConsumer
from kafka.errors import KafkaError,  NoBrokersAvailable

logger = logging.getlogger(__name__)



class StockConsumer : 
    def __init__(
            self,
            topic : str,
        config  : KafkaConfig | None = None) -> None:
        self.topic = topic,
        self.config = config or KafkaConfig()
        self._consumer = KafkaConsumer | None = None
       



    def run(self) -> None:    
        logger.info("="*75)
        logger.info("Consuming clean stocks from kafka ....")
        self.consume_stocks()


    def start(self) -> None:
        try:
            self._consumer = KafkaConsumer(
                bootstrap_server = self.config.bootstrap_servers,
                value_serializer = lambda x: x.decode('utf-8'),
                
            )
            logger.info("Kafka consumer initiated...")

        except NoBrokersAvailable as exc:
            raise ConnectionError(
                f"Could not connect to Kafka at {self.config.bootstrap_servers}. "
                "Is the broker running?"
            ) from exc    


     



    def consume_stocks(config: KafkaConfig) -> None:
        stock_consumer = StockConsumer()

        
        