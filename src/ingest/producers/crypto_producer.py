import asyncio
import logging
import signal


from .base_producer import BaseProducer
from config.kafka_config import KafkaConfig
from config.api_config import BinanceConfig
from ..sources.binance_ws import BinanceWebSocket

logger = logging.getLogger(__name__)


DEFAULT_SYMBOLS = [
    "BTCUSDT","ETHUSDT","SOLUSDT"
]


class CryptoProducer(BaseProducer):

    def __init__(
            self,
            symbols: list[str]   = DEFAULT_SYMBOLS,
            kafka_config: KafkaConfig | None = None,
            binance_config: BinanceConfig | None = None,
            interval:   str    = "1m",

    ) -> None:
        super().__init__(
            topic= (kafka_config or KafkaConfig()).RAW_CRYPTO_TOPIC,
            config = kafka_config,
        )
        self.symbols   = [s.upper() for s in symbols]
        self.binance_config = binance_config or BinanceConfig()
        self.interval = interval
        self._source: BinanceWebSocket | None = None

    async def start(self) -> None:
        self.binance_config.validate()
        self.connect()
        logger.info("CryptoProducer validaated and connected to Kafka.")

        self._source = BinanceWebSocket(
            symbols = self.symbols,
            callback= self._on_tick ,
            interval = self.interval,
            emit_open = False,         #only closed candles to Kafka
            config = self.binance_config,

        )

        logger.info(
            "CryptoProducer starting | symbols=%s interval=%s topic=%s",
            self.symbols, self.interval, self.topic,

        )

        try:
            await self._source.run()
        finally:
            self.close()    

    def stop(self) -> None:

        logger.info("CryptoProducer stop requested.")
        if self._source:
            self._source.stop()

    async def _on_tick(self, tick: dict) -> None:
        logger.debug(
            "Crypto tick | %s  O=%.2f H=%.2f L=%.2f C=%.2f vol=%d",
            tick["symbol"], tick["open"], tick["high"],
            tick["low"],    tick["close"], tick["volume"],
        ) 
        self.publish(tick)   

async def _main() -> None:
        producer = CryptoProducer()

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

         