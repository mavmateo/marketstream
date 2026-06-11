import asyncio
import logging
import signal
 
from .base_producer import BaseProducer
from config.kafka_config import KafkaConfig
from config.api_config import AlpacaConfig
from ..sources.alpaca_ws import AlpacaWebSocket




logger = logging.getLogger(__name__)


DEFAULT_SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "TSLA", "META", "JPM", "V", "UNH",
]


class StockProducer(BaseProducer):

    def __init__(
        self,
        symbols:       list[str]          = DEFAULT_SYMBOLS,
        kafka_config:  KafkaConfig  | None = None,
        alpaca_config: AlpacaConfig | None = None,
        interval:      str                = "1Min",
    ) -> None:
        super().__init__(
            topic  = (kafka_config or KafkaConfig()).RAW_STOCKS_TOPIC,
            config = kafka_config,
        )
        self.symbols       = [s.upper() for s in symbols]
        self.alpaca_config = alpaca_config or AlpacaConfig()
        self.interval      = interval
        self._source: AlpacaWebSocket | None = None

    async def start(self) -> None:

        self.alpaca_config.validate()   
        self.connect()                  
 
        self._source = AlpacaWebSocket(
            symbols  = self.symbols,
            callback = self._on_tick,
            interval = self.interval,
            config   = self.alpaca_config,
        )
 
        logger.info(
            "StockProducer starting | symbols=%s interval=%s topic=%s",
            self.symbols, self.interval, self.topic,
        )
 
        try:
            await self._source.run()    # blocks until source stops
        finally:
            self.close()                # flush + close Kafka on any exit
 
    def stop(self) -> None:
         
        logger.info("StockProducer stop requested.")
        if self._source:
            self._source.stop()

    async def _on_tick(self, tick: dict) -> None:
         
        logger.debug(
            "Stock tick | %s  O=%.2f H=%.2f L=%.2f C=%.2f vol=%d",
            tick["symbol"], tick["open"], tick["high"],
            tick["low"],    tick["close"], tick["volume"],
        )
        self.publish(tick) 

async def _main() -> None:
    producer = StockProducer()
 
    
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, producer.stop)
 
    await producer.start()  

if __name__ == "__main__":
    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    asyncio.run(_main())                 