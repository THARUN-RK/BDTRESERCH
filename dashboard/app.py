"""Auto-refreshing local dashboard backed by the FastAPI analytics service."""

from __future__ import annotations

import os
from typing import Any

import plotly.express as px
import requests
import streamlit as st

API_BASE_URL = os.environ.get(
    "GAMING_ANALYTICS_API_URL", "http://127.0.0.1:8000"
).rstrip("/")
REFRESH_SECONDS = 5


def _get_json(path: str) -> Any:
    try:
        response = requests.get(f"{API_BASE_URL}{path}", timeout=3.0)
    except requests.exceptions.RequestException as error:
        st.error(f"Cannot reach the analytics API at {API_BASE_URL}: {error}")
        st.info(
            "Start it in another PowerShell window with "
            "`py -3.13 -m uvicorn backend.app:app --host 127.0.0.1 --port 8000`."
        )
        st.stop()

    if not response.ok:
        st.error(
            f"Analytics API returned HTTP {response.status_code} for {path}."
        )
        st.code(response.text[:1000])
        st.stop()

    try:
        return response.json()
    except ValueError as error:
        st.error(f"Analytics API returned invalid JSON for {path}: {error}")
        st.stop()


def _as_list(payload: Any, endpoint: str) -> list[dict[str, Any]]:
    if not isinstance(payload, list) or any(
        not isinstance(item, dict) for item in payload
    ):
        st.error(f"Unexpected response shape from {endpoint}; expected a list of objects.")
        st.stop()
    return payload


def _format_score(value: int | float | None) -> str:
    return "—" if value is None else f"{value:,.1f}"


def _render_dashboard() -> None:
    overview = _get_json("/api/metrics/overview")
    activity = _as_list(_get_json("/api/metrics/activity?minutes=60"), "activity")
    daily = _as_list(_get_json("/api/metrics/daily?days=7"), "daily statistics")
    leaderboard = _as_list(_get_json("/api/leaderboard?limit=10"), "leaderboard")
    sessions = _as_list(_get_json("/api/sessions?limit=20"), "sessions")
    quality = _get_json("/api/data-quality")
    kafka = _get_json("/api/kafka/status")

    if not isinstance(overview, dict) or not isinstance(quality, dict) or not isinstance(kafka, dict):
        st.error("Analytics API returned an unexpected response shape.")
        st.stop()

    st.title("Gaming Analytics")
    st.caption(
        f"Live view of locally stored gaming events · refreshes every {REFRESH_SECONDS} seconds"
    )

    metrics = [
        ("Total players", f"{overview['total_players']:,}", "Players seen in SQLite"),
        ("Active players", f"{overview['active_players']:,}", "Latest stored login state"),
        ("Gaming sessions", f"{overview['total_sessions']:,}", "Stored session records"),
        ("Average score", _format_score(overview["average_score"]), "Across stored score events"),
        ("Highest score", _format_score(overview["highest_score"]), "Highest stored score event"),
        (
            "Events / second",
            f"{overview['events_per_second']:.2f}",
            overview["events_rate_basis"],
        ),
        (
            "Kafka processing latency",
            "Not measured"
            if overview["kafka_processing_latency_ms"] is None
            else f"{overview['kafka_processing_latency_ms']:.2f} ms",
            overview["kafka_latency_note"],
        ),
    ]

    for columns, row_metrics in (
        (st.columns(4), metrics[:4]),
        (st.columns(3), metrics[4:]),
    ):
        for column, (label, value, help_text) in zip(columns, row_metrics):
            column.metric(label, value, help=help_text)

    st.caption(
        f"{overview['events_in_last_60_seconds']:,} stored events in the last "
        f"60 seconds · {overview['total_processed_events']:,} total"
    )

    st.subheader("Player activity · last 60 minutes")
    if activity:
        chart = px.area(
            activity,
            x="minute",
            y="event_count",
            color="event_type",
            markers=True,
            labels={
                "minute": "Minute (UTC)",
                "event_count": "Events",
                "event_type": "Event type",
            },
            template="plotly_dark",
        )
        chart.update_layout(
            margin=dict(l=12, r=12, t=18, b=12),
            legend_title_text="Event type",
            hovermode="x unified",
        )
        st.plotly_chart(chart, width="stretch", config={"displayModeBar": False})
    else:
        st.info("No processed events in the selected period yet.")

    daily_columns = st.columns(2)
    with daily_columns[0]:
        st.subheader("Daily gaming activity")
        daily_chart = px.bar(
            daily,
            x="day",
            y="events",
            labels={"day": "Date (UTC)", "events": "Processed events"},
            template="plotly_dark",
        )
        daily_chart.update_layout(margin=dict(l=12, r=12, t=18, b=12))
        st.plotly_chart(
            daily_chart, width="stretch", config={"displayModeBar": False}
        )
    with daily_columns[1]:
        st.subheader("Sessions started per day")
        session_chart = px.bar(
            daily,
            x="day",
            y="sessions_started",
            labels={"day": "Date (UTC)", "sessions_started": "Sessions"},
            template="plotly_dark",
        )
        session_chart.update_layout(margin=dict(l=12, r=12, t=18, b=12))
        st.plotly_chart(
            session_chart, width="stretch", config={"displayModeBar": False}
        )
    score_days = [point for point in daily if point["average_score"] is not None]
    st.subheader("Daily average score")
    if score_days:
        score_chart = px.line(
            score_days,
            x="day",
            y="average_score",
            markers=True,
            labels={"day": "Date (UTC)", "average_score": "Average score"},
            template="plotly_dark",
        )
        score_chart.update_layout(margin=dict(l=12, r=12, t=18, b=12))
        st.plotly_chart(
            score_chart, width="stretch", config={"displayModeBar": False}
        )
    else:
        st.info("No score events have been stored in the selected daily window.")

    table_columns = st.columns(2)
    with table_columns[0]:
        st.subheader("Player leaderboard")
        if leaderboard:
            st.dataframe(
                leaderboard,
                width="stretch",
                hide_index=True,
                column_config={
                    "player_id": "Player",
                    "total_score": st.column_config.NumberColumn("Total score"),
                    "score_events": st.column_config.NumberColumn("Score events"),
                    "highest_score": st.column_config.NumberColumn("Best score"),
                },
            )
        else:
            st.info("No scores have been stored yet.")
    with table_columns[1]:
        st.subheader("Gaming session analysis")
        if sessions:
            st.dataframe(
                sessions,
                width="stretch",
                hide_index=True,
                column_config={
                    "session_id": st.column_config.TextColumn("Session", width="small"),
                    "player_id": "Player",
                    "game_id": "Game",
                    "started_at": "Started (UTC)",
                    "ended_at": "Ended (UTC)",
                    "duration_seconds": st.column_config.NumberColumn(
                        "Duration (s)", format="%.1f"
                    ),
                    "status": "Status",
                },
            )
        else:
            st.info("No gaming sessions have been stored yet.")

    st.subheader("Data quality")
    quality_columns = st.columns(3)
    quality_columns[0].metric("Valid", f"{quality['valid']:,}")
    quality_columns[1].metric("Invalid", f"{quality['invalid']:,}")
    quality_columns[2].metric("Duplicate", f"{quality['duplicate']:,}")
    quality_values = [
        {"status": status, "records": quality[status]}
        for status in ("valid", "invalid", "duplicate")
        if quality[status] > 0
    ]
    if quality_values:
        quality_chart = px.pie(
            quality_values,
            names="status",
            values="records",
            hole=0.55,
            template="plotly_dark",
        )
        quality_chart.update_layout(margin=dict(l=12, r=12, t=18, b=12))
        st.plotly_chart(
            quality_chart, width="stretch", config={"displayModeBar": False}
        )
    else:
        st.info("No event-quality records have been stored yet.")

    st.subheader("Kafka consumer monitoring")
    if kafka["broker_reachable"]:
        st.success(f"Broker reachable at {kafka['bootstrap_servers']}")
    else:
        st.warning(
            f"Broker is not reachable at {kafka['bootstrap_servers']}. "
            "Start the local KRaft broker to stream events."
        )
    st.caption(
        f"Consumer group: {kafka['consumer_group']} · "
        f"Lag: {kafka['consumer_lag'] if kafka['consumer_lag'] is not None else 'Not measured'}"
    )
    st.caption(kafka["consumer_lag_note"])


st.set_page_config(
    page_title="Gaming Analytics",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded",
)

with st.sidebar:
    st.title("Gaming Analytics")
    st.caption("Apache Kafka research dashboard")
    st.markdown(f"API: `{API_BASE_URL}`")
    st.caption(f"Auto-refresh: every {REFRESH_SECONDS} seconds")
    st.divider()
    st.caption("Only values stored in the local SQLite database are displayed.")


@st.fragment(run_every=f"{REFRESH_SECONDS}s")
def dashboard_fragment() -> None:
    _render_dashboard()


dashboard_fragment()
