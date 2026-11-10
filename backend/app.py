"""Read-only analytics API for gaming events stored in SQLite."""

from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, Query
from pydantic import BaseModel

from database import Database
from kafka.settings import KafkaSettings


class HealthResponse(BaseModel):
    status: Literal["ok"]
    database: Literal["connected"]


class OverviewResponse(BaseModel):
    total_players: int
    active_players: int
    total_sessions: int
    average_score: float | None
    highest_score: int | None
    events_per_second: float
    events_in_last_60_seconds: int
    total_processed_events: int
    events_rate_basis: str
    kafka_processing_latency_ms: float | None
    kafka_latency_note: str


class ActivityPoint(BaseModel):
    minute: str
    event_type: str
    event_count: int


class DailyStatisticsPoint(BaseModel):
    day: str
    events: int
    sessions_started: int
    average_score: float | None


class LeaderboardEntry(BaseModel):
    player_id: str
    total_score: int
    score_events: int
    highest_score: int


class SessionEntry(BaseModel):
    session_id: str
    player_id: str
    game_id: str
    started_at: str | None
    ended_at: str | None
    duration_seconds: float | None
    status: Literal["active", "ended"]


class DataQualityResponse(BaseModel):
    valid: int
    invalid: int
    duplicate: int


class KafkaStatusResponse(BaseModel):
    broker_reachable: bool
    bootstrap_servers: str
    consumer_group: str
    consumer_lag: int | None
    consumer_lag_note: str


def _parse_bootstrap_servers(bootstrap_servers: str) -> list[tuple[str, int]]:
    addresses: list[tuple[str, int]] = []
    for address in bootstrap_servers.split(","):
        candidate = address.strip()
        if candidate.startswith("["):
            closing_bracket = candidate.find("]")
            if closing_bracket < 0 or candidate[closing_bracket + 1 : closing_bracket + 2] != ":":
                raise ValueError(f"invalid Kafka bootstrap address: {candidate}")
            host = candidate[1:closing_bracket]
            port_text = candidate[closing_bracket + 2 :]
        else:
            host, separator, port_text = candidate.rpartition(":")
            if not separator:
                raise ValueError(f"invalid Kafka bootstrap address: {candidate}")

        try:
            port = int(port_text)
        except ValueError as error:
            raise ValueError(f"invalid Kafka bootstrap port: {candidate}") from error
        if not host or not 1 <= port <= 65535:
            raise ValueError(f"invalid Kafka bootstrap address: {candidate}")
        addresses.append((host, port))
    if not addresses:
        raise ValueError("at least one Kafka bootstrap address is required")
    return addresses


def _is_broker_reachable(addresses: list[tuple[str, int]]) -> bool:
    for host, port in addresses:
        try:
            with socket.create_connection((host, port), timeout=0.25):
                return True
        except OSError:
            continue
    return False


def create_app(
    database_path: str | Path | None = None,
    bootstrap_servers: str | None = None,
    consumer_group: str | None = None,
) -> FastAPI:
    """Create the API and initialize the lightweight local database."""
    database_location = database_path or os.environ.get(
        "GAMING_ANALYTICS_DB", "data/gaming_analytics.sqlite3"
    )
    kafka_settings = KafkaSettings(
        bootstrap_servers=bootstrap_servers
        or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        consumer_group=consumer_group
        or os.environ.get("KAFKA_CONSUMER_GROUP", "gaming-analytics"),
    )
    broker_addresses = _parse_bootstrap_servers(kafka_settings.bootstrap_servers)
    database = Database(database_location)

    app = FastAPI(
        title="Gaming Analytics API",
        description=(
            "Read-only API for validated gaming events persisted in local SQLite. "
            "Kafka latency and consumer lag are reported as unavailable until measured."
        ),
        version="0.1.0",
    )
    app.state.database = database
    app.state.kafka_settings = kafka_settings
    app.state.kafka_addresses = broker_addresses

    @app.get("/", include_in_schema=False)
    def api_root() -> dict[str, str]:
        return {
            "name": "Gaming Analytics API",
            "docs": "/docs",
            "health": "/health",
        }

    @app.get("/health", response_model=HealthResponse, tags=["health"])
    def health() -> HealthResponse:
        database.table_counts()
        return HealthResponse(status="ok", database="connected")

    @app.get("/api/metrics/overview", response_model=OverviewResponse, tags=["analytics"])
    def overview() -> dict[str, object]:
        return {
            **database.overview(),
            "events_rate_basis": (
                "Stored event timestamps in the last 60 seconds; not broker throughput."
            ),
            "kafka_latency_note": (
                "Kafka processing latency has not been instrumented yet."
            ),
        }

    @app.get("/api/metrics/activity", response_model=list[ActivityPoint], tags=["analytics"])
    def activity(
        minutes: int = Query(default=60, ge=1, le=1440),
    ) -> list[dict[str, str | int]]:
        return database.activity(minutes)

    @app.get(
        "/api/metrics/daily",
        response_model=list[DailyStatisticsPoint],
        tags=["analytics"],
    )
    def daily_statistics(
        days: int = Query(default=7, ge=1, le=365),
    ) -> list[dict[str, str | int | float | None]]:
        return database.daily_statistics(days)

    @app.get("/api/leaderboard", response_model=list[LeaderboardEntry], tags=["analytics"])
    def leaderboard(
        limit: int = Query(default=10, ge=1, le=100),
    ) -> list[dict[str, str | int]]:
        return database.leaderboard(limit)

    @app.get("/api/sessions", response_model=list[SessionEntry], tags=["analytics"])
    def sessions(
        limit: int = Query(default=20, ge=1, le=100),
    ) -> list[dict[str, object]]:
        return database.recent_sessions(limit)

    @app.get(
        "/api/data-quality",
        response_model=DataQualityResponse,
        tags=["monitoring"],
    )
    def data_quality() -> dict[str, int]:
        return database.quality_counts()

    @app.get(
        "/api/kafka/status",
        response_model=KafkaStatusResponse,
        tags=["monitoring"],
    )
    def kafka_status() -> KafkaStatusResponse:
        return KafkaStatusResponse(
            broker_reachable=_is_broker_reachable(broker_addresses),
            bootstrap_servers=kafka_settings.bootstrap_servers,
            consumer_group=kafka_settings.consumer_group,
            consumer_lag=None,
            consumer_lag_note=(
                "Consumer-group lag is not instrumented yet; this endpoint only "
                "checks TCP reachability."
            ),
        )

    return app


app = create_app()
