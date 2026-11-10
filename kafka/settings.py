from dataclasses import dataclass
import math


@dataclass(frozen=True, slots=True)
class KafkaSettings:
    bootstrap_servers: str = "localhost:9092"
    events_topic: str = "gaming-events"
    dead_letter_topic: str = "gaming-events-dlq"
    consumer_group: str = "gaming-analytics"
    partitions: int = 3
    replication_factor: int = 1
    processing_attempts: int = 3
    retry_backoff_seconds: float = 0.25

    def __post_init__(self) -> None:
        if not self.bootstrap_servers.strip():
            raise ValueError("bootstrap_servers cannot be empty")
        if not self.events_topic or not self.dead_letter_topic:
            raise ValueError("Kafka topic names cannot be empty")
        if self.events_topic == self.dead_letter_topic:
            raise ValueError("events_topic and dead_letter_topic must be different")
        if self.partitions < 1:
            raise ValueError("partitions must be at least 1")
        if self.replication_factor < 1:
            raise ValueError("replication_factor must be at least 1")
        if self.processing_attempts < 1:
            raise ValueError("processing_attempts must be at least 1")
        if (
            not math.isfinite(self.retry_backoff_seconds)
            or self.retry_backoff_seconds < 0
        ):
            raise ValueError("retry_backoff_seconds must be finite and non-negative")
