```python
import os
import psycopg2

from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime, timezone


# ==========================================
# GAMING ANALYTICS API
# ==========================================

app = FastAPI(
    title="Gaming Analytics API",
    description="Real-time gaming analytics using Apache Kafka, Python and Neon PostgreSQL",
    version="4.0"
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
# NEON POSTGRESQL CONFIGURATION
# ==========================================

DATABASE_URL = os.getenv("DATABASE_URL")


def get_connection():

    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL environment variable is missing"
        )

    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor,
        connect_timeout=10
    )


# ==========================================
# DATABASE INITIALIZATION
# ==========================================

def initialize_database():

    conn = get_connection()

    try:
        with conn.cursor() as cursor:

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gaming_events (

                    id BIGSERIAL PRIMARY KEY,

                    player_id TEXT NOT NULL,

                    event_type TEXT NOT NULL,

                    score INTEGER DEFAULT 0,

                    event_timestamp TIMESTAMPTZ,

                    kafka_partition INTEGER,

                    kafka_offset BIGINT,

                    received_at TIMESTAMPTZ
                        DEFAULT CURRENT_TIMESTAMP
                )
            """)

        conn.commit()

    finally:
        conn.close()


@app.on_event("startup")
def startup_event():

    initialize_database()


# ==========================================
# EVENT INPUT MODEL
# ==========================================

class GamingEvent(BaseModel):

    player_id: str

    event_type: str

    score: int = 0

    event_timestamp: datetime | None = None

    kafka_partition: int | None = None

    kafka_offset: int | None = None


# ==========================================
# HOME API
# ==========================================

@app.get("/")
def home():

    return {
        "message": "Gaming Analytics API is running",

        "status": "online",

        "version": "4.0",

        "database": "Neon PostgreSQL",

        "technology": [
            "Apache Kafka",
            "Python",
            "PostgreSQL",
            "FastAPI",
            "Streamlit"
        ]
    }


# ==========================================
# HEALTH CHECK
# ==========================================

@app.get("/health")
def health():

    try:

        conn = get_connection()

        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")

        conn.close()

        return {
            "status": "healthy",
            "database": "connected",
            "service": "Gaming Analytics API",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    except Exception:
        raise HTTPException(
            status_code=503,
            detail="Database connection failed"
        )


# ==========================================
# RECEIVE GAMING EVENTS
# ==========================================

@app.post("/events")
def receive_event(event: GamingEvent):

    conn = get_connection()

    try:

        with conn.cursor() as cursor:

            cursor.execute("""
                INSERT INTO gaming_events (

                    player_id,
                    event_type,
                    score,
                    event_timestamp,
                    kafka_partition,
                    kafka_offset

                )

                VALUES (%s, %s, %s, %s, %s, %s)

                RETURNING id

            """, (

                event.player_id,
                event.event_type,
                event.score,
                event.event_timestamp or datetime.now(timezone.utc),
                event.kafka_partition,
                event.kafka_offset

            ))

            result = cursor.fetchone()

        conn.commit()

        return {
            "status": "success",
            "message": "Gaming event stored in Neon PostgreSQL",
            "event_id": result["id"],
            "player_id": event.player_id
        }

    except psycopg2.Error:

        conn.rollback()

        raise HTTPException(
            status_code=500,
            detail="Failed to store gaming event"
        )

    finally:

        conn.close()


# ==========================================
# GET GAMING EVENTS
# ==========================================

@app.get("/events")
def get_events(
    limit: int = Query(default=100, ge=1, le=1000)
):

    conn = get_connection()

    try:

        with conn.cursor() as cursor:

            cursor.execute("""
                SELECT *
                FROM gaming_events
                ORDER BY id DESC
                LIMIT %s
            """, (limit,))

            rows = cursor.fetchall()

        return rows

    finally:

        conn.close()


# ==========================================
# GAMING STATISTICS
# ==========================================

@app.get("/stats")
def get_stats():

    conn = get_connection()

    try:

        with conn.cursor() as cursor:

            cursor.execute("""
                SELECT

                    COUNT(*) AS total_events,

                    COUNT(DISTINCT player_id)
                    AS total_players,

                    COUNT(*) FILTER (
                        WHERE event_type = 'kill'
                    ) AS total_kills,

                    COUNT(*) FILTER (
                        WHERE event_type = 'death'
                    ) AS total_deaths,

                    COALESCE(AVG(score), 0)
                    AS average_score

                FROM gaming_events
            """)

            result = cursor.fetchone()

        return {
            "total_events": result["total_events"],
            "total_players": result["total_players"],
            "total_kills": result["total_kills"],
            "total_deaths": result["total_deaths"],
            "average_score": round(
                float(result["average_score"]), 2
            )
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

        with conn.cursor() as cursor:

            cursor.execute("""
                SELECT

                    player_id,

                    COUNT(*) AS total_events,

                    COUNT(*) FILTER (
                        WHERE event_type = 'kill'
                    ) AS total_kills,

                    COUNT(*) FILTER (
                        WHERE event_type = 'death'
                    ) AS total_deaths,

                    COALESCE(AVG(score), 0)
                    AS average_score

                FROM gaming_events

                GROUP BY player_id

                ORDER BY total_events DESC

            """)

            rows = cursor.fetchall()

        result = []

        for player in rows:

            kills = player["total_kills"] or 0

            deaths = player["total_deaths"] or 0

            player["kd_ratio"] = round(
                kills / deaths if deaths > 0 else float(kills),
                2
            )

            player["average_score"] = round(
                float(player["average_score"]), 2
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

        with conn.cursor() as cursor:

            cursor.execute("""
                SELECT

                    COUNT(*) AS total_events,

                    COUNT(*) FILTER (
                        WHERE received_at >=
                        NOW() - INTERVAL '60 seconds'
                    ) AS events_last_60_seconds,

                    COALESCE(
                        AVG(
                            EXTRACT(
                                EPOCH FROM (
                                    received_at - event_timestamp
                                )
                            ) * 1000
                        ) FILTER (
                            WHERE received_at IS NOT NULL
                            AND event_timestamp IS NOT NULL
                        ),
                        0
                    ) AS average_latency_ms

                FROM gaming_events
            """)

            result = cursor.fetchone()

        recent_events = result["events_last_60_seconds"]

        throughput = recent_events / 60

        return {

            "total_events": result["total_events"],

            "events_last_60_seconds": recent_events,

            "throughput_events_per_second": round(
                throughput, 2
            ),

            "average_latency_ms": round(
                max(0, float(result["average_latency_ms"])), 2
            ),

            "measurement_window_seconds": 60

        }

    finally:

        conn.close()


# ==========================================
# END OF GAMING ANALYTICS API
# ==========================================
```