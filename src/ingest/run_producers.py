import asyncio
import logging
import signal


from .producers.stock_producer  import StockProducer
from .producers.crypto_producer import CryptoProducer

logger = logging.getLogger(__name__)



def _build_producers() -> tuple:
    stock_producer = StockProducer()
    crypto_producer = CryptoProducer()
    return stock_producer, crypto_producer

def _shutdown(producers: tuple) -> None:
    logger.info("Shutdown signal received - stopping all producers")
    for p in producers:
        p.stop()




async def _run() -> None:
    producers = _build_producers()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: _shutdown(producers))
    
    
    logger.info("Both Stock Producer and Crypto Producer Starting..."),
    results = await asyncio.gather(
        producers[0].start(),
        producers[1].start(),
        return_exceptions = True
        )
    
    for producer, result in zip(producers, results):
        if isinstance(result, Exception):
            logger.error("Producer %s exited with error: %s", 
                         producer.__class__.__name__, result)
    logger.info("="*50)        
    logger.info("All producers stopped")   
    logger.info("="*50)     
    
    
    
        


if __name__ == "__main__": 
    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    asyncio.run(_run())