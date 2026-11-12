# gamedev-trends

Trend radar for indie game developers.

Status: so far this is only the measurement contract and the event bus (Kafka and in-memory).
There are no collectors, storage or API yet.

## Development

```bash
uv sync
cp .env.example .env
docker compose up -d --wait   # Kafka, ClickHouse, Redis
uv run pytest                 # unit tests, no Docker needed
uv run pytest -m integration  # needs the compose stack (Kafka on localhost:9092)
docker compose down
```
