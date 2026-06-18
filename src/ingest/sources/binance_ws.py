import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Callable, Awaitable

from logger_setup import get_config
from logger_setup import get_logger

from config.api_config import BinanceConfig 
 
import websockets
from websockets.exceptions import (
    ConnectionClosedError,
    ConnectionClosedOK,
    WebSocketException,
)

logger = get_config()
logger = get_logger(__name__)

_BASE_URL_ = "wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1m/ethusdt@kline_1m/solusdt@kline_1m"

_BACKOFF_INITIAL      = 1      
_BACKOFF_MULTIPLIER   = 2      
_BACKOFF_CAP          = 60     
_BACKOFF_RESET_AFTER  = 300 


def _parse_kline(msg: dict) -> dict | None:
    try:
        data = msg.get("data", {})
        event = data.get("e")

        if event != "kline":
            return None
        
        k       = data["k"]
        symbol  = k["s"]
        ts_ms   = k["t"]


        return {
            "symbol":    symbol,
            "market":    "crypto",
            "timestamp": datetime.fromtimestamp(
                             ts_ms / 1000, tz=timezone.utc
                         ).isoformat(),
            "open":      float(k["o"]),
            "high":      float(k["h"]),
            "low":       float(k["l"]),
            "close":     float(k["c"]),
            "price":     float(k["c"]),          
            "volume":    float(k["v"]),
            "trades":    int(k["n"]),
            "closed":    bool(k["x"]),           
            "interval":  k["i"],
        }

    except (KeyError, ValueError, TypeError) as exc:

        logger.warning("Failed to parse kline message: %s | msg=%s", exc, msg)  
        return None
    

class BinanceWebSocket:

    def __init__(
            self,
            symbols: list[str],
            callback:  Callable[[dict], Awaitable[None]],
            interval: str = "1m",
            emit_open: bool = False,
            config: BinanceConfig | None = None,
            
            ) -> None:
                if not symbols:
                     raise ValueError("At least one symbol must be specified.")
                
                self.symbols   = [s.upper() for s in symbols]
                self.callback  = callback
                self.interval  = interval
                self.emit_open = emit_open
                self.config    = config or BinanceConfig()


                self._url         = _BASE_URL_
                self._running     = False
                self._tick_count  = 0
                self._connect_time: float | None = None

                logger.info("="*75)
                logger.info(
                      "BinanceWebSocket initialised | symbols=%s interval=%s emit_open=%s",
                       self.symbols, self.interval, self.emit_open,)
                logger.info("="*75)
    async def run(self) -> None:
         self._running = True
         backoff       = _BACKOFF_INITIAL
         attempt       = 0

         while self._running:
              attempt += 1
              logger.info("="*75)
              logger.info(
                   "Connecting to Binance stream (attempt %d) | url=%s",
                attempt, self._url,
              ) 
              logger.info("="*75)     

              try:
                   async with websockets.connect(
                        self._url,
                        ping_interval=20,
                        ping_timeout=10,
                        close_timeout=5,
                   )   as ws:
                        self._connect_time = time.monotonic()
                        logger.info("="*75)
                        logger.info("Binance Websocket connected.") 
                        logger.info("="*75)
                        await self._listen(ws)

              except ConnectionClosedOK: 
                   logger.info("="*75)
                   logger.info("Binance Websocket closed cleanly.") 
                   logger.info("="*75)
                   break

              except ConnectionClosedError as exc:
                   logger.info("="*75)
                   logger.warning("Binance connection dropped: %s", exc) 
                   logger.info("="*75)  

              except WebSocketException as exc:
                logger.error("Binance WebSocket error: %s", exc)
 
              except OSError as exc:
                logger.error("Network error connecting to Binance: %s", exc)     

              if not self._running:
                   break  


              elapsed = (
                   time.monotonic() - self._connect_time
                   if self._connect_time else 0
              )         
              if elapsed >= _BACKOFF_RESET_AFTER:
                   backoff = _BACKOFF_INITIAL
                   logger.info("Back-off counter reset after clean run.")

              else:
                   backoff = min(backoff * _BACKOFF_MULTIPLIER, _BACKOFF_CAP)

              logger.info("Reconnecting in %s seconds ...", backoff)
              await asyncio.sleep(backoff)  

         logger.info("="*75)
         logger.info(
               "BinanceWebSocket stopped. Total ticks emitted: %d", self._tick_count
         )  
         logger.info("="*75)      

    def stop(self) -> None:
         logger.info("="*75)
         logger.info("Stop requested on BinanceWebSocket.")
         logger.info("="*75)
         self._running = False


    async def _listen(self, ws) -> None:
       
        async for raw in ws:
            if not self._running:
                break
 
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("Received non-JSON message from Binance: %s", raw[:120])
                continue
 
            tick = _parse_kline(msg)
            if tick is None:
                continue
 
            if not self.emit_open and not tick["closed"]:
                
                continue
 
            self._tick_count += 1
 
            logger.debug(
                "Tick #%d | %s price=%.4f closed=%s",
                self._tick_count, tick["symbol"], tick["price"], tick["closed"],
            )
 
            try:
                await self.callback(tick)
            except Exception as exc:
                
                logger.error(
                    "Callback raised an exception for tick %s: %s",
                    tick["symbol"], exc, exc_info=True,
                )    
        

async def _smoke_test() -> None:
     

     received: list[dict] = []

     async def on_tick(tick: dict) -> None:
          received.append(tick)
          print(
            f"[{tick['timestamp']}] {tick['symbol']:10s} "
            f"O={tick['open']:.2f}  H={tick['high']:.2f}  "
            f"L={tick['low']:.2f}   C={tick['close']:.2f}  "
            f"vol={tick['volume']:.3f}"
          )

          if len(received) >= 5:
               ws_instance.stop()

     ws_instance = BinanceWebSocket(
          symbols= ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
          callback = on_tick,
          interval = "1m",
          emit_open= True,
     )

     await ws_instance.run()
     print(f"\nSmoke test complete. Received {len(received)} ticks.")          


if __name__ == "__main__":
    logging.basicConfig(
        level = logging.INFO,
        format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    asyncio.run(_smoke_test())