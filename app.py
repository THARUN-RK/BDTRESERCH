from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
from datetime import datetime

# ==========================================
# GAMING ANALYTICS API
# ==========================================

app = FastAPI(
    title="Gaming Analytics API",
    description="Real-time gaming analytics using Apache Kafka, Python and SQLite",
    version="2.0"
)

# ==========================================
# CORS CONFIGURATION
# ==========================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"]
)

# ==========================================
# DATABASE CONFIGURATION
# ==========================================

DATABASE = "gaming_analytics.db"


def get_connection():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn
def initialize_database():
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gaming_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                score INTEGER DEFAULT 0,
                event_timestamp TEXT,
                kafka_partition INTEGER,
                kafka_offset INTEGER,
                received_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()


initialize_database()


# ==========================================
# HOME API
# ==========================================

@app.get("/")
def home():
    return {
        "message": "Gaming Analytics API is running",
        "status": "online",
        "technology": [
            "Apache Kafka",
            "Python",
            "SQLite",
            "FastAPI",
            "Streamlit"
        ]
    }


# ==========================================
# HEALTH CHECK
# ==========================================

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "service": "Gaming Analytics API",
        "timestamp": datetime.now().isoformat()
    }


# ==========================================
# GET GAMING EVENTS
# ==========================================

@app.get("/events")
def get_events(
    limit: int = Query(default=100, ge=1, le=1000)
):

    conn = get_connection()

    try:
        rows = conn.execute("""
            SELECT *
            FROM gaming_events
            ORDER BY id DESC
            LIMIT ?
        """, (limit,)).fetchall()

        return [dict(row) for row in rows]

    finally:
        conn.close()


# ==========================================
# GAMING STATISTICS
# ==========================================

@app.get("/stats")
def get_stats():

    conn = get_connection()

    try:
        total_events = conn.execute("""
            SELECT COUNT(*)
            FROM gaming_events
        """).fetchone()[0]

        total_players = conn.execute("""
            SELECT COUNT(DISTINCT player_id)
            FROM gaming_events
        """).fetchone()[0]

        total_kills = conn.execute("""
            SELECT COUNT(*)
            FROM gaming_events
            WHERE event_type = 'kill'
        """).fetchone()[0]

        total_deaths = conn.execute("""
            SELECT COUNT(*)
            FROM gaming_events
            WHERE event_type = 'death'
        """).fetchone()[0]

        average_score = conn.execute("""
            SELECT AVG(score)
            FROM gaming_events
        """).fetchone()[0]

        return {
            "total_events": total_events,
            "total_players": total_players,
            "total_kills": total_kills,
            "total_deaths": total_deaths,
            "average_score": round(average_score or 0, 2)
        }

    finally:
        conn.close()


# ==========================================
# PLAYER ANALYTICS
# ==========================================

@app.get("/players")
def get_players():

    conn = get_connection()

    try:
        rows = conn.execute("""
            SELECT
                player_id,
                COUNT(*) AS total_events,

                SUM(
                    CASE
                        WHEN event_type = 'kill'
                        THEN 1 ELSE 0
                    END
                ) AS total_kills,

                SUM(
                    CASE
                        WHEN event_type = 'death'
                        THEN 1 ELSE 0
                    END
                ) AS total_deaths,

                AVG(score) AS average_score

            FROM gaming_events

            GROUP BY player_id

            ORDER BY total_events DESC

        """).fetchall()

        result = []

        for row in rows:
            player = dict(row)

            kills = player["total_kills"] or 0
            deaths = player["total_deaths"] or 0

            player["kd_ratio"] = round(
                kills / deaths if deaths > 0 else float(kills),
                2
            )

            player["average_score"] = round(
                player["average_score"] or 0,
                2
            )

            result.append(player)

        return result

    finally:
        conn.close()


# ==========================================
# KAFKA PERFORMANCE METRICS
# ==========================================

@app.get("/metrics")
def get_metrics():

    conn = get_connection()

    try:
        # Total events
        total_events = conn.execute("""
            SELECT COUNT(*)
            FROM gaming_events
        """).fetchone()[0]

        # Events received in the last 60 seconds
        recent_events = conn.execute("""
            SELECT COUNT(*)
            FROM gaming_events
            WHERE julianday(received_at) >=
                  julianday('now', 'localtime', '-60 seconds')
        """).fetchone()[0]

        # Average event-processing latency
        avg_latency = conn.execute("""
            SELECT AVG(
                (julianday(received_at) -
                 julianday(event_timestamp)) * 86400000
            )
            FROM gaming_events
            WHERE received_at IS NOT NULL
              AND event_timestamp IS NOT NULL
        """).fetchone()[0]

        throughput = recent_events / 60

        return {
            "total_events": total_events,
            "events_last_60_seconds": recent_events,
            "throughput_events_per_second": round(
                throughput, 2
            ),
            "average_latency_ms": round(
                max(0, avg_latency or 0), 2
            ),
            "measurement_window_seconds": 60
        }

    finally:
        conn.close()


# ==========================================
# END OF API
# ==========================================