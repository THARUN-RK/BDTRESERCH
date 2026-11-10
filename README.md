# Large-Scale Online Gaming Data Processing and Data Visualization Dashboard Using Apache Kafka

A resource-conscious research project for streaming simulated gaming events, processing them, storing results, and presenting analytics.

## Project status

Phases 1–4: the environment, project structure, event generator, and Kafka producer/consumer code are in place. The live Kafka end-to-end check still requires a local broker. Preprocessing, SQLite persistence, the API, and the dashboard have not yet been implemented.

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
