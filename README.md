# Large-Scale Online Gaming Data Processing and Data Visualization Dashboard Using Apache Kafka

> **Live Dashboard & Research Site:** [https://tharun-rk.github.io/BDTRESERCH/](https://tharun-rk.github.io/BDTRESERCH/)

---

## 📄 Abstract

**Abstract—** In the last few years, online gaming platforms have been booming, resulting in a massive increase in data generated from player actions, events, scores, transactions, and interactions with the system. Traditional approaches to handling and analysing such high volume and high speed data can be a challenge, especially if real-time monitoring and visualization is needed. Previous works exist regarding the use of Apache Kafka for collecting and reporting real-time game data, preprocessing and streaming and visualization of high volume of data. An integrated approach that is able to effectively process and visualise online game data, which encompasses all these stages, is still needed. To address this, in this proposed research we are developing a big data online game data preprocessing and visualization system based on Apache Kafka. The proposed system will employ Kafka to continuously ingest the gaming events and organize the data being ingested from the various topics in the streaming app. Data preprocessing will be done before analyzing data which includes data cleaning, data validation, elimination of data repetitions, missing data, filtering, and data transformation. Data will then be consolidated then displayed on a real-time visualization dashboard. The dashboard will also offer valuable data on the player's activity, game performance, scores, game sessions, how often they participate in games and other relevant gaming metrics. The goal of this system is to create an end-to-end pipeline starting from large scale game-data collection, preprocessing and visualization. This is to facilitate the effective processing of data streams and provide real-time monitoring of gaming activities. The proposed system will be analyzed based on the data-processing latency, throughput, efficiency of pre-processing, and responsiveness of the visualization. This will likely result in a scalable and efficient solution that will convert raw online gaming data into valuable visual data for real-time analysis and decision-making.

**Keywords:** `Big Data Technologies`, `Apache Kafka`, `Real-Time Streaming`, `Data Preprocessing`, `Data Deduplication`, `FastAPI`, `Streamlit Dashboard`, `Online Gaming Telemetry`.

### 🎯 Research Evaluation Metrics
1. **Data-Processing Latency:** End-to-end millisecond processing latency from event generation to persistence in SQLite / Neon PostgreSQL.
2. **System Throughput:** High-speed event ingestion rate across partitioned Kafka topics.
3. **Pre-Processing Efficiency:** Validation, schema enforcement, deduplication accuracy, and DLQ routing.
4. **Visualization Responsiveness:** Real-time rendering latency and 5-second interval refresh responsiveness.

---

## Project status

Phases 1–8: the environment, project structure, event generator, Kafka producer/consumer code, event validation/deduplication, SQLite persistence, FastAPI analytics API, and local Streamlit dashboard are in place. The live Kafka end-to-end check still requires a local broker.

The GitHub Pages site is a static project overview. It does not host the Python application, Kafka broker, or live analytics dashboard.

## Planned technology stack

- Python 3.13
- Apache Kafka in KRaft mode
- FastAPI and Streamlit
- SQLite, Pandas, and NumPy
- Plotly

## Project directories

- `event_generator/`: simulated player and game events
- `kafka/`: Kafka configuration and streaming integration
- `preprocessing/`: validation, cleaning, and duplicate handling
- `database/`: SQLite storage and data access
- `backend/`: FastAPI application
- `dashboard/`: Streamlit analytics dashboard
- `analytics/`: metrics and analysis
- `experiments/`: reproducible performance experiments
- `tests/`: automated tests
- `data/`: local datasets and generated data
- `docs/`: project and research documentation

## Development

Follow the project phases in order. Do not treat performance results as measured until experiments have actually been run and their outputs recorded.

### Generate a finite JSONL sample (Windows PowerShell)

From the project directory:

```powershell
py -3.13 -m event_generator.generator --count 100 --rate 20 --players 50 --seed 42 --output .\data\gaming-events.jsonl
```

The command reports the generated event count on standard error. Each line in the output file is one independent JSON event. Use `--count 0` to run continuously; press Ctrl+C to stop.

### Run the tests

```powershell
py -3.13 -m unittest discover -s tests -v
```

### Validate and deduplicate events

`preprocessing.EventPreprocessor` validates the event type, identifiers, timestamp, and event-specific fields; normalizes timestamps to UTC; and counts valid, invalid, and duplicate events. The Kafka consumer runs this step before accepting a message; invalid records go to the dead-letter topic and duplicate IDs are not printed as accepted events. SQLite adds persistent event-ID deduplication and data-quality records across restarts.

### Initialize and inspect the SQLite database

The consumer initializes `data\gaming_analytics.sqlite3` automatically. To initialize it or inspect current row counts without starting Kafka:

```powershell
py -3.13 -c "from database import Database; db = Database(); print(db.table_counts()); print(db.quality_counts())"
```

SQLite stores players, sessions, scores, transactions, processed events, and valid/invalid/duplicate quality records. Generated database files are ignored by Git.

### Start the FastAPI backend

From the project directory:

```powershell
py -3.13 -m uvicorn backend.app:app --host 127.0.0.1 --port 8000
```

Open `http://127.0.0.1:8000/docs` for the interactive API documentation. In another PowerShell window, verify the service and its real SQLite-backed responses:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
Invoke-RestMethod http://127.0.0.1:8000/api/metrics/overview
Invoke-RestMethod http://127.0.0.1:8000/api/data-quality
Invoke-RestMethod http://127.0.0.1:8000/api/kafka/status
```

The API also exposes `/api/metrics/activity`, `/api/metrics/daily`, `/api/leaderboard`, and `/api/sessions`. Kafka reachability is a TCP check only. Consumer lag and Kafka processing latency return as unavailable until they are actually instrumented; event rate is calculated from stored event timestamps and is not reported as broker throughput.

### Start the Streamlit dashboard

Keep the FastAPI window running and open another PowerShell window in the project directory:

```powershell
py -3.13 -m streamlit run dashboard/app.py
```

Open `http://localhost:8501`. The dashboard reads real SQLite-backed API responses and refreshes every five seconds. With an empty database, it shows empty-state messages and zero stored counts; it does not fill the charts with sample metrics. GitHub Pages remains a static project site and cannot host the live Python/Kafka/SQLite application.

### Produce and consume Kafka events

Start the Kafka KRaft broker in a separate PowerShell window first. Then, from the project directory, start the consumer:

```powershell
py -3.13 -m kafka.consumer --max-messages 20
```

In another PowerShell window, produce events:

```powershell
py -3.13 -m kafka.producer --count 20 --rate 5 --players 10 --seed 42
```

The producer creates `gaming-events` and `gaming-events-dlq` with three partitions and a replication factor of one for a single-broker laptop setup. Events are keyed by player ID. The consumer uses manual offset commits and the `gaming-analytics` consumer group; transient processing failures are retried and then sent to the dead-letter topic before their offsets are committed.

The client uses bounded producer (16 MB) and consumer (8 MB) queues to avoid large buffering on the 8 GB laptop. Kafka-side broker memory is separately limited by the Phase 1 configuration. If an event cannot be decoded or processing exhausts its retries, its source topic, partition, offset, error, and base64-encoded original value are written to the DLQ.

For the complete Windows KRaft setup and the order for starting the broker, consumer, API, dashboard, and continuous producer, follow the [local dashboard startup guide](https://tharun-rk.github.io/BDTRESERCH/#dashboard-setup). The single-node broker configuration is in `kafka/server.properties`; its data directory is relative to the Kafka installation directory.
