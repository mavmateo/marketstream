
import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class KafkaConfig:
 
    bootstrap_servers: list[str] = field(
        default_factory=lambda: os.getenv(
            "KAFKA_BROKER", "localhost:9092"
        ).split(",")
    )

    RAW_STOCKS_TOPIC:   str = "raw.stocks"
    RAW_CRYPTO_TOPIC:   str = "raw.crypto"
    CLEAN_STOCKS_TOPIC: str = "clean.stocks"
    CLEAN_CRYPTO_TOPIC: str = "clean.crypto"


    linger_ms:         int = 10

    retries:           int = 5

    acks:              str = "all"

    consumer_group_ai:       str = "ai-pipeline"
    consumer_group_dashboard: str = "dashboard-feed"
    auto_offset_reset:        str = "latest"   

    def validate(self) -> None:
        if not self.bootstrap_servers:
            raise EnvironmentError("KAFKA_BROKER env var must be set.")