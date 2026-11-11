import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from streamlit_autorefresh import st_autorefresh

# =====================================
# CONFIGURATION
# =====================================

st.set_page_config(
    page_title="Gaming Analytics Platform",
    page_icon="🎮",
    layout="wide"
)

# DEPLOYED FASTAPI URL
API_URL = "https://gaming-analytics-api.onrender.com"

st_autorefresh(interval=5000, key="gaming_refresh")

# =====================================
# API CONNECTION
# =====================================

@st.cache_data(ttl=3)
def get_data(endpoint):
    try:
        response = requests.get(
            f"{API_URL}/{endpoint}",
            timeout=30
        )
        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        st.error(f"API Error: {e}")
        return None


stats = get_data("stats")
players = get_data("players")
events = get_data("events?limit=500")
metrics = get_data("metrics")

# =====================================
# HEADER
# =====================================

st.title("🎮 Gaming Analytics Platform")

st.caption(
    "Large-Scale Online Gaming Data Processing "
    "and Visualization using Apache Kafka"
)

if stats is None:
    st.error("FastAPI connection failed.")
    st.stop()
else:
    st.success("Dashboard connected to deployed FastAPI!")

# =====================================
# GAMING KPI
# =====================================

st.header("📊 Gaming Overview")

c1, c2, c3, c4, c5 = st.columns(5)

c1.metric("Total Events", stats["total_events"])
c2.metric("Total Players", stats["total_players"])
c3.metric("Total Kills", stats["total_kills"])
c4.metric("Total Deaths", stats["total_deaths"])
c5.metric("Average Score", stats["average_score"])

st.divider()

# =====================================
# KAFKA PERFORMANCE
# =====================================

st.header("⚡ Kafka Performance Monitoring")

if metrics:

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "Total Processed",
        metrics["total_events"]
    )

    c2.metric(
        "Recent Events",
        metrics["events_last_60_seconds"]
    )

    c3.metric(
        "Throughput",
        str(metrics["throughput_events_per_second"])
        + " events/sec"
    )

    c4.metric(
        "Average Latency",
        str(metrics["average_latency_ms"])
        + " ms"
    )

else:
    st.warning("Performance metrics unavailable.")

st.divider()

# =====================================
# PLAYER ANALYTICS
# =====================================

st.header("🏆 Player Performance")

if players:

    df_players = pd.DataFrame(players)

    col1, col2 = st.columns(2)

    with col1:

        fig = px.bar(
            df_players,
            x="player_id",
            y="total_kills",
            title="Player Kill Statistics",
            text="total_kills"
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

    with col2:

        fig = px.bar(
            df_players,
            x="player_id",
            y="average_score",
            title="Average Player Score",
            text="average_score"
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

    st.subheader("Player Leaderboard")

    st.dataframe(
        df_players.sort_values(
            by="total_kills",
            ascending=False
        ),
        use_container_width=True
    )

else:
    st.info("Waiting for player data.")

st.divider()

# =====================================
# EVENT DISTRIBUTION
# =====================================

st.header("📈 Gaming Event Distribution")

if events:

    df_events = pd.DataFrame(events)

    col1, col2 = st.columns(2)

    with col1:

        event_counts = (
            df_events["event_type"]
            .value_counts()
            .reset_index()
        )

        event_counts.columns = [
            "Event Type",
            "Count"
        ]

        fig = px.pie(
            event_counts,
            names="Event Type",
            values="Count",
            title="Event Type Distribution"
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

    with col2:

        fig = px.histogram(
            df_events,
            x="score",
            nbins=20,
            title="Gaming Score Distribution"
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

    # =================================
    # EVENT TIMELINE
    # =================================

    st.subheader("Gaming Activity Timeline")

    df_events["received_at"] = pd.to_datetime(
        df_events["received_at"],
        errors="coerce"
    )

    timeline = (
        df_events.dropna(subset=["received_at"])
        .set_index("received_at")
        .resample("10s")
        .size()
        .reset_index(name="event_count")
    )

    if not timeline.empty:

        fig = px.line(
            timeline,
            x="received_at",
            y="event_count",
            markers=True,
            title="Gaming Events Over Time"
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

    # =================================
    # RECENT EVENTS
    # =================================

    st.subheader("Recent Gaming Events")

    st.dataframe(
        df_events[
            [
                "player_id",
                "event_type",
                "score",
                "event_timestamp",
                "received_at"
            ]
        ],
        use_container_width=True
    )

else:
    st.info("No gaming events available.")

# =====================================
# FOOTER
# =====================================

st.divider()

st.caption(
    "Architecture: Python → Kafka → SQLite "
    "→ FastAPI → Streamlit"
)

st.caption(
    "Data source: Simulated gaming events. "
    "Dashboard refresh interval: 5 seconds."
)
