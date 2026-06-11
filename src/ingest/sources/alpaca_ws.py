import asyncio
import json 
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Callable, Awaitable




from config.api_config import AlpacaConfig

import websockets
from websockets.exceptions import (
    ConnectionClosedError,
    ConnectionClosedOK,
    WebSocketException,
)

logger = logging.getLogger(__name__)



_BASE_URL = "wss://stream.data.alpaca.markets/v2/iex"

_BACKOFF_INITIAL   = 1
_BACKOFF_MULTIPLIER = 2
_BACKOFF_CAP        = 60
_BACKOFF_RESET_AFTER = 300

def _parse_bar(msg: list) -> list[dict]:
    
        bars = []
        for item in msg:
            if item.get("T") != "b":
                continue
            try:
                bars.append({
                    "symbol":   item["S"],
                    "market":   "stock",
                    "timestamp": item["t"],
                    "open":  float(item["o"]),
                    "high":  float(item["h"]),
                    "low"  : float(item["l"]),
                    "close" : float(item["c"]),
                    "volume" : float(item["v"]),
                    "trades" :  int(item.get("n",0)),
                    "vwap" :   float(item.get("vw", 0)),
                    "closed" : True
                })
        
       
    
            except (KeyError, ValueError, TypeError) as exc:
                logger.warning("Failed to parse bar: %s | item=%s", exc, item)
        return bars
    


class AlpacaWebSocket:

    def __init__(
            self,
            symbols:  list[str],
            callback:  Callable[[dict], Awaitable[None]],
            interval: str = "1m",
            config: AlpacaConfig | None = None,

      ) -> None:
        if not symbols:
            raise ValueError("At least one symbol must be specified")
        


        self.symbols = [s.upper() for s in symbols]
        self.callback = callback
        self.interval  = interval
        self.config   = config or AlpacaConfig()


        self._url         = _BASE_URL
        self._running      = False
        self._bar_count  = 0
        self._connect_time: float | None = None

        
        logger.info(
            "AlpacaWebsocket initiated | symbols=%s interval=%s",
            self.symbols, self.interval,
            
        )

    async def run(self) -> None:
        self._running   = True
        backoff         = _BACKOFF_INITIAL
        attempt         = 0

        while self._running:
            attempt += 1
            logger.info(
                "Connecting to Alpaca stream (attempt %d) | url=%s",
                attempt, self._url,
            )

            try:
                async with websockets.connect(
                    self._url,
                    ping_interval = 20,
                    ping_timeout = 10,
                    close_timeout=5,
                ) as ws:
                    self._connect_time = time.monotonic()
                    logger.info("Alpaca Websocket connected.")

                    await ws.recv()

                    await ws.send(json.dumps({
                    "action": "auth",
                    "key" : self.config.api_key,
                    "secret" : self.config.secret_key,
                    }))

                    response = json.loads(await ws.recv())
                    if not any(m.get("msg") == "authenticated" for m in response):
                        raise ConnectionError(f"Alpaca auth failed: {response}")
                    
                    
                    await ws.send(json.dumps({
                        "action": "subscribe",
                        "bars" : self.symbols
                    }))

                    sub_response = json.loads(await ws.recv())
                    logger.info("Subscription confirmed: %s", sub_response)

                    await self._listen(ws)

            except ConnectionClosedOK:
                logger.info("Alpaca Websocket closed cleanly.")
                break

            except ConnectionClosedError as exc:
                logger.warning("Alpaca connection dropped: %s", exc) 

            except WebSocketException as exc:
                logger.error("Alpaca WebSocker error: %s", exc)

            except OSError as exc:
                logger.error("Network error connecting to Alpaca: %s", exc)

            
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

        logger.info(
            "AlpacaWebSocket stopped. Total bars emitted: %d", self._bar_count
        )                            
    
    def stop(self) -> None:
        logger.info("Stop requested on AlpacaWebSocket.")
        self._running = False
        
    async def _listen(self, ws) -> None:

        async for raw in ws:
            if not self._running:
                break

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("Received non-JSON data from Alpaca: %s", raw[:120])
                continue
            
            bars = _parse_bar(msg)
            for bar in bars:
                self._bar_count += 1
                await self.callback(bar)


def _market_is_open() -> bool:
        ET = timezone(timedelta(hours=-4))
        now = datetime.now(ET)
        if now.weekday() >= 5:
            return False
        market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
        return market_open <= now <= market_close
           


async def _smoke_test() -> None:
    if not _market_is_open():
        ET = timezone(timedelta(hours=-4))
        now = datetime.now(ET)
        print(f"Market is closed. Current ET time: {now.strftime('%H:%M')}.")
        print("Market opens Mon-Fri at 09:30 ET (13:30 Ghana time).")
        return

    received: list[dict] = []

    async def on_bar(bar: dict) -> None:
        received.append(bar)
        print(
            f"[{bar['timestamp']}] {bar['symbol']:10s} "
            f"O={bar['open']:.2f} H={bar['high']:.2f} "
            f"L={bar['low']:.2f}  C={bar['close']:.2f} "
            f"vol={bar['volume']:.3f}"
        )

        if len(received) >= 5:
            ws_instance.stop()


    ws_instance = AlpacaWebSocket(
        symbols=["MSFT", "GOOG", "AMZN"],
        callback= on_bar,
        interval = "1m",

    )

    await ws_instance.run()
    print(f"\nSmoke test complete. Received {len(received)} bars.") 

   
    





if __name__ == "__main__":
     logging.basicConfig(
     level = logging.INFO,
     format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s",

     )

     asyncio.run(_smoke_test())
