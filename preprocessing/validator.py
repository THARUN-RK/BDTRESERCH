"""Validate, normalize, and deduplicate gaming events before persistence."""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import TypeAlias

from event_generator.generator import ACTIVITY_ACTIONS, GAME_IDS

GamingEvent: TypeAlias = dict[str, object]


class EventStatus(StrEnum):
    VALID = "valid"
    INVALID = "invalid"
    DUPLICATE = "duplicate"


@dataclass(frozen=True, slots=True)
class PreprocessingResult:
    status: EventStatus
    event: GamingEvent | None
    reason: str | None = None


@dataclass(slots=True)
class PreprocessingStats:
    valid: int = 0
    invalid: int = 0
    duplicate: int = 0

    @property
    def total(self) -> int:
        return self.valid + self.invalid + self.duplicate


class _InvalidEvent(ValueError):
    pass


class EventPreprocessor:
    """Validate event fields and track duplicate IDs for this process lifetime.

    The in-memory ID set is suitable for local development and bounded tests.
    Phase 6 will move durable duplicate detection to SQLite.
    """

    _EVENT_TYPES = {
        "player_login",
        "player_logout",
        "game_start",
        "game_end",
        "player_score",
        "transaction",
        "player_activity",
        "game_error",
    }

    def __init__(self) -> None:
        self._seen_event_ids: set[str] = set()
        self.stats = PreprocessingStats()

    def process(self, raw_event: object) -> PreprocessingResult:
        """Return a normalized event or a counted invalid/duplicate result."""
        try:
            event = self._normalize_and_validate(raw_event)
        except _InvalidEvent as error:
            self.stats.invalid += 1
            return PreprocessingResult(EventStatus.INVALID, None, str(error))

        event_id = event["event_id"]
        if not isinstance(event_id, str):
            raise RuntimeError("Validated event ID unexpectedly changed type")
        if event_id in self._seen_event_ids:
            self.stats.duplicate += 1
            return PreprocessingResult(
                EventStatus.DUPLICATE, event, "event_id has already been processed"
            )

        self._seen_event_ids.add(event_id)
        self.stats.valid += 1
        return PreprocessingResult(EventStatus.VALID, event)

    def _normalize_and_validate(self, raw_event: object) -> GamingEvent:
        if not isinstance(raw_event, dict):
            raise _InvalidEvent("event must be a JSON object")

        event: GamingEvent = dict(raw_event)
        event_id = self._uuid_field(event, "event_id")
        event_type = self._string_field(event, "event_type")
        if event_type not in self._EVENT_TYPES:
            raise _InvalidEvent(f"unsupported event_type: {event_type}")

        player_id = self._string_field(event, "player_id")
        event["event_id"] = event_id
        event["event_type"] = event_type
        event["player_id"] = player_id
        event["timestamp"] = self._normalized_timestamp(event)

        if event_type in {"game_start", "game_end", "player_score"}:
            event["session_id"] = self._uuid_field(event, "session_id")
            event["game_id"] = self._game_id_field(event)
        elif event_type == "player_logout":
            if "session_id" in event:
                event["session_id"] = self._uuid_field(event, "session_id")
                event["game_id"] = self._game_id_field(event)
                event["session_duration_seconds"] = self._duration_field(event)
            elif "game_id" in event or "session_duration_seconds" in event:
                raise _InvalidEvent(
                    "logout session details require a session_id"
                )

        if event_type == "game_end":
            event["session_duration_seconds"] = self._duration_field(event)
        elif event_type == "player_logout" and "session_duration_seconds" in event:
            event["session_duration_seconds"] = self._duration_field(event)
        elif event_type == "player_score":
            score = event.get("score")
            if isinstance(score, bool) or not isinstance(score, int) or score < 0:
                raise _InvalidEvent("score must be a non-negative integer")
        elif event_type == "transaction":
            event["transaction_id"] = self._uuid_field(event, "transaction_id")
            amount = event.get("amount_cents")
            if isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
                raise _InvalidEvent("amount_cents must be a positive integer")
            currency = self._string_field(event, "currency").upper()
            if len(currency) != 3 or not currency.isalpha() or not currency.isascii():
                raise _InvalidEvent("currency must be a three-letter ASCII code")
            event["currency"] = currency
        elif event_type == "player_activity":
            action = self._string_field(event, "action")
            if action not in ACTIVITY_ACTIONS:
                raise _InvalidEvent(f"unsupported activity action: {action}")
        elif event_type == "game_error":
            self._string_field(event, "error_code")
            event["game_id"] = self._game_id_field(event)
            if "session_id" in event:
                event["session_id"] = self._uuid_field(event, "session_id")

        return event

    @staticmethod
    def _string_field(event: GamingEvent, field: str) -> str:
        value = event.get(field)
        if not isinstance(value, str) or not value.strip():
            raise _InvalidEvent(f"{field} must be a non-empty string")
        return value.strip()

    @classmethod
    def _uuid_field(cls, event: GamingEvent, field: str) -> str:
        value = cls._string_field(event, field)
        try:
            return str(uuid.UUID(value))
        except ValueError as error:
            raise _InvalidEvent(f"{field} must be a valid UUID") from error

    @classmethod
    def _game_id_field(cls, event: GamingEvent) -> str:
        game_id = cls._string_field(event, "game_id")
        if game_id not in GAME_IDS:
            raise _InvalidEvent(f"unsupported game_id: {game_id}")
        return game_id

    @classmethod
    def _duration_field(cls, event: GamingEvent) -> float:
        value = event.get("session_duration_seconds")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise _InvalidEvent("session_duration_seconds must be a finite non-negative number")
        try:
            duration = float(value)
        except OverflowError as error:
            raise _InvalidEvent(
                "session_duration_seconds must be a finite non-negative number"
            ) from error
        if not math.isfinite(duration) or duration < 0:
            raise _InvalidEvent("session_duration_seconds must be a finite non-negative number")
        return duration

    @classmethod
    def _normalized_timestamp(cls, event: GamingEvent) -> str:
        value = cls._string_field(event, "timestamp")
        try:
            timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise _InvalidEvent("timestamp must be an ISO 8601 datetime") from error
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise _InvalidEvent("timestamp must include a timezone")
        return timestamp.astimezone(timezone.utc).isoformat()
