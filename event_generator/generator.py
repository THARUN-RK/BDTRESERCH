"""Generate realistic, lightweight gaming events as JSON-compatible dictionaries."""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
import time
import uuid
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO

EVENT_WEIGHTS = {
    "player_login": 12,
    "player_logout": 7,
    "game_start": 18,
    "game_end": 12,
    "player_score": 20,
    "transaction": 10,
    "player_activity": 16,
    "game_error": 5,
}

GAME_IDS = ("arena", "puzzle", "racing", "strategy")
ACTIVITY_ACTIONS = ("move", "chat", "matchmaking", "item_use")
ERROR_CODES = ("GAME_TIMEOUT", "CONNECTION_LOST", "ASSET_LOAD_FAILED")


class GamingEventGenerator:
    """Create events while maintaining online-player and active-session state."""

    def __init__(self, players: int = 100, seed: int | None = None) -> None:
        if players < 1:
            raise ValueError("players must be at least 1")

        self._rng = random.Random(seed)
        self._player_ids = tuple(f"player-{number:05d}" for number in range(1, players + 1))
        self._online_players: set[str] = set()
        self._active_sessions: dict[str, tuple[str, datetime, str]] = {}

    def generate_event(self) -> dict[str, object]:
        """Generate one event, choosing only actions valid for the current state."""
        offline = [player for player in self._player_ids if player not in self._online_players]
        online = sorted(self._online_players)
        available_sessions = sorted(self._active_sessions)
        players_without_sessions = [
            player for player in online if player not in self._active_sessions
        ]

        choices: list[tuple[str, int, Sequence[str]]] = []
        if offline:
            choices.append(("player_login", EVENT_WEIGHTS["player_login"], offline))
        if online:
            choices.extend(
                [
                    ("player_logout", EVENT_WEIGHTS["player_logout"], online),
                    ("transaction", EVENT_WEIGHTS["transaction"], online),
                    ("player_activity", EVENT_WEIGHTS["player_activity"], online),
                ]
            )
        if players_without_sessions:
            choices.append(
                ("game_start", EVENT_WEIGHTS["game_start"], players_without_sessions)
            )
        if available_sessions:
            choices.extend(
                [
                    ("game_end", EVENT_WEIGHTS["game_end"], available_sessions),
                    ("player_score", EVENT_WEIGHTS["player_score"], available_sessions),
                ]
            )
        choices.append(("game_error", EVENT_WEIGHTS["game_error"], self._player_ids))

        event_type = self._rng.choices(
            [choice[0] for choice in choices],
            weights=[choice[1] for choice in choices],
            k=1,
        )[0]
        eligible_players = next(choice[2] for choice in choices if choice[0] == event_type)
        player_id = self._rng.choice(eligible_players)
        timestamp = datetime.now(timezone.utc)

        event: dict[str, object] = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "timestamp": timestamp.isoformat(),
            "player_id": player_id,
        }

        if event_type == "player_login":
            self._online_players.add(player_id)
        elif event_type == "player_logout":
            self._online_players.remove(player_id)
            session = self._active_sessions.pop(player_id, None)
            if session:
                session_id, started_at, game_id = session
                event.update(
                    {
                        "session_id": session_id,
                        "game_id": game_id,
                        "session_duration_seconds": max(
                            0.0, (timestamp - started_at).total_seconds()
                        ),
                    }
                )
        elif event_type == "game_start":
            game_id = self._rng.choice(GAME_IDS)
            session_id = str(uuid.uuid4())
            self._active_sessions[player_id] = (session_id, timestamp, game_id)
            event.update({"session_id": session_id, "game_id": game_id})
        elif event_type == "game_end":
            session_id, started_at, game_id = self._active_sessions.pop(player_id)
            event.update(
                {
                    "session_id": session_id,
                    "game_id": game_id,
                    "session_duration_seconds": max(
                        0.0, (timestamp - started_at).total_seconds()
                    ),
                }
            )
        elif event_type == "player_score":
            session_id, _, game_id = self._active_sessions[player_id]
            event.update(
                {
                    "session_id": session_id,
                    "game_id": game_id,
                    "score": self._rng.randint(0, 1000),
                }
            )
        elif event_type == "transaction":
            event.update(
                {
                    "transaction_id": str(uuid.uuid4()),
                    "amount_cents": self._rng.randint(99, 4999),
                    "currency": "USD",
                }
            )
        elif event_type == "player_activity":
            event["action"] = self._rng.choice(ACTIVITY_ACTIONS)
        elif event_type == "game_error":
            event.update(
                {
                    "error_code": self._rng.choice(ERROR_CODES),
                    "game_id": self._rng.choice(GAME_IDS),
                }
            )
            session = self._active_sessions.get(player_id)
            if session:
                event["session_id"] = session[0]

        return event

    def run(
        self,
        count: int,
        events_per_second: float,
        sink: Callable[[dict[str, object]], None],
    ) -> int:
        """Generate up to count events at a target rate; count=0 runs until stopped."""
        if count < 0:
            raise ValueError("count cannot be negative")
        if not math.isfinite(events_per_second) or events_per_second <= 0:
            raise ValueError("events_per_second must be greater than zero")

        interval = 1.0 / events_per_second
        next_event_at = time.monotonic()
        generated = 0

        while count == 0 or generated < count:
            sink(self.generate_event())
            generated += 1
            if count and generated >= count:
                break

            next_event_at += interval
            delay = next_event_at - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            else:
                next_event_at = time.monotonic()

        return generated


def _positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def _nonnegative_int(value: str) -> int:
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return number


def _positive_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return number


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate gaming events as newline-delimited JSON."
    )
    parser.add_argument(
        "--count",
        type=_nonnegative_int,
        default=1000,
        help="number of events to generate; use 0 to run continuously (default: 1000)",
    )
    parser.add_argument(
        "--rate",
        type=_positive_float,
        default=10.0,
        help="target generation rate in events per second (default: 10)",
    )
    parser.add_argument(
        "--players",
        type=_positive_int,
        default=100,
        help="size of the simulated player pool (default: 100)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="optional random seed for repeatable event choices",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="optional JSONL output file; defaults to standard output",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    generator = GamingEventGenerator(players=args.players, seed=args.seed)

    output: TextIO
    close_output = args.output is not None
    output = args.output.open("w", encoding="utf-8") if args.output else sys.stdout
    try:
        generated = generator.run(
            count=args.count,
            events_per_second=args.rate,
            sink=lambda event: output.write(
                json.dumps(event, separators=(",", ":")) + "\n"
            ),
        )
        output.flush()
    finally:
        if close_output:
            output.close()

    print(f"Generated {generated} events.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
