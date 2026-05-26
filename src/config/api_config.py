import os
import logging
from dataclasses import dataclass, field
from dotenv import load_dotenv

from src.logger_setup import get_config
from src.logger_setup import get_logger


logger = logging.getLogger(__name__)

load_dotenv()

@dataclass

class BinanceConfig:
    api_key:      str = field(default_factory=lambda: os.getenv("BINANCE_API_KEY", ""))
    api_secret:   str = field(default_factory=lambda: os.getenv("BINANCE_SECRET_KEY", ""))

    @property
    def ws_headers(self) -> dict:

        if self.api_key:
            return {"X-MBX-APIKEY": self.api_key}
        return {}
    
    def validate(self) -> None:
        if not self.api_key:
            logger.warning(
                "BINANCE_API_KEY not set. Authenticated streams will be unavailable."
            )

@dataclass
class AlpacaConfig:
    api_key:    str = field(default_factory=lambda: os.getenv("ALPACA_API_KEY", ""))
    secret_key: str = field(default_factory=lambda: os.getenv("ALPACA_SECRET_KEY", ""))
    base_url:   str = field(default_factory=lambda: os.getenv(
                                "ALPACA_WS_URL",
                                "wss://stream.data.alpaca.markets/v2/iex"
                            ))
    feed:       str = field(default_factory=lambda: os.getenv("ALPACA_FEED", "iex"))
 
    @property
    def ws_headers(self) -> dict:
        return {}   # Alpaca auth is sent as a JSON message after connect
 
    def validate(self) -> None:
        missing = [k for k, v in {
            "ALPACA_API_KEY":    self.api_key,
            "ALPACA_SECRET_KEY": self.secret_key,
        }.items() if not v]
        if missing:
            raise EnvironmentError(
                f"Missing required Alpaca credentials: {missing}"
            )
