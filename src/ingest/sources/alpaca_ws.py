

import asyncio
import logging
from typing import Callable, Awaitable

from alpaca.data.live import StockDataStream

from config.api_config import AlpacaConfig

logger = logging.getLogger(__name__)


class AlpacaWebSocket:
   

    def __init__(
        self,
        symbols:  list[str],
        callback: Callable[[dict], Awaitable[None]],
        interval: str = "1Min",
        config:   AlpacaConfig | None = None,
    ) -> None:
        self.symbols  = [s.upper() for s in symbols]
        self.callback = callback
        self.interval = interval
        self.config   = config or AlpacaConfig()

        
        self._stream  = StockDataStream(
            self.config.api_key,
            self.config.secret_key,
        )
        self._running = False

    async def run(self) -> None:
        self._running = True
        self._stream.subscribe_bars(self._on_bar, *self.symbols)
        logger.info("Alpaca stream started | symbols=%s", self.symbols)
        await self._stream._run_forever()

    def stop(self) -> None:
        self._running = False
        self._stream.stop()

    async def _on_bar(self, bar) -> None:
        tick = {
            "symbol":    bar.symbol,
            "market":    "stock",
            "timestamp": bar.timestamp.isoformat(),
            "open":      float(bar.open),
            "high":      float(bar.high),
            "low":       float(bar.low),
            "close":     float(bar.close),
            "price":     float(bar.close),
            "volume":    float(bar.volume),
            "trades":    getattr(bar, "trade_count", 0),
            "closed":    True,
            "interval":  self.interval,
        }
        await self.callback(tick)