

import json
import logging
from abc import ABC, abstractmethod

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from config.kafka_config import KafkaConfig

from src.logger_setup import get_config
from src.logger_setup import get_logger


logger = get_config(__name__)


class BaseProducer(ABC):
   

    def __init__(self, topic: str, config: KafkaConfig | None = None) -> None:
        self.topic  = topic
        self.config = config or KafkaConfig()
        self._producer: KafkaProducer | None = None
        self._published_count = 0

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def connect(self) -> None:
       
        logger.info(
            "Connecting Kafka producer | topic=%s brokers=%s",
            self.topic, self.config.bootstrap_servers,
        )
        try:
            self._producer = KafkaProducer(
                bootstrap_servers  = self.config.bootstrap_servers,
                value_serializer   = self._serialise,
                acks               = self.config.acks,
                retries            = self.config.retries,
                linger_ms          = self.config.linger_ms,
                # Compress batches — cuts Kafka storage and network by ~60%
                # for JSON tick data
                compression_type   = "gzip",
            )
            logger.info("Kafka producer connected.")
        except NoBrokersAvailable as exc:
            raise ConnectionError(
                f"Could not connect to Kafka at {self.config.bootstrap_servers}. "
                "Is the broker running?"
            ) from exc

    def close(self) -> None:
        
        if self._producer:
            logger.info(
                "Closing Kafka producer | total published=%d", self._published_count
            )
            self._producer.flush()
            self._producer.close()
            self._producer = None

    # ── Publishing ────────────────────────────────────────────────────────────

    def publish(self, tick: dict) -> None:
     
        if self._producer is None:
            raise RuntimeError("Producer not connected. Call connect() first.")

        key = tick.get("symbol", "UNKNOWN").encode("utf-8")

        self._producer.send(
            self.topic,
            value    = tick,
            key      = key,
        ).add_callback(
            self._on_send_success
        ).add_errback(
            self._on_send_error
        )

        self._published_count += 1

    # ── Callbacks ─────────────────────────────────────────────────────────────

    def _on_send_success(self, record_metadata) -> None:
        logger.debug(
            "Published | topic=%s partition=%d offset=%d",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )

    def _on_send_error(self, exc: KafkaError) -> None:
        logger.error("Failed to publish message to Kafka: %s", exc)

    # ── Serialisation ─────────────────────────────────────────────────────────

    @staticmethod
    def _serialise(value: dict) -> bytes:
        """JSON-encode a tick dict to UTF-8 bytes for Kafka."""
        return json.dumps(value, default=str).encode("utf-8")

    # ── Abstract interface ────────────────────────────────────────────────────

    @abstractmethod
    async def start(self) -> None:
       
        ...