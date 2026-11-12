# gamedev-trends

Trend radar for indie game developers.

Status: the measurement contract, the event bus (Kafka and in-memory) and the Steam collector exist.
There is no storage or API yet.

## Development

```bash
uv sync
cp .env.example .env
docker compose up -d --wait   # Kafka, ClickHouse, Redis
uv run pytest                 # unit tests, no Docker needed
uv run pytest -m integration  # needs the compose stack (Kafka on localhost:9092, Redis on localhost:6379)
docker compose down
```

## Steam collector

Once per slot (by default every hour, at the start of the hour, UTC) the collector takes the Steam
most-played chart, asks for the current player count of every game in the top, and publishes
`Observation` events to `radar.observations.v1`: `chart_rank` and `chart_peak_players` (the chart is a
daily summary, so these are stamped with its rollup date and repeated runs produce the same `event_id`)
and `players_current` (stamped with the slot). A game without player data gets no `players_current`
event. Games are polled one at a time. Every request takes a token from a token bucket in Redis shared
by all collector instances; 429, 5xx, timeouts and connection errors are retried with exponential
backoff, and a game whose request finally fails only loses its own measurement.

The Steam Web API allows 100 000 calls per day; the top 100 once an hour is 101 × 24 = 2 424 calls.

Run one slot from the host (needs the compose stack for Kafka and Redis):

```bash
docker compose up -d --wait kafka kafka-init redis
uv run python -m gamedev_trends.steam --once
```

Run it as a service, which collects at start and then at the start of every hour:

```bash
docker compose up -d --wait
```

Configuration is read from environment variables; all have defaults. An invalid value makes the
process exit with code 2.

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis for the shared rate limiter |
| `STEAM_API_BASE` | `https://api.steampowered.com` | Steam Web API base URL |
| `STEAM_POLL_INTERVAL_SECONDS` | `3600` | Slot length in seconds, at least 60 and a divisor of 86400 |
| `STEAM_TOP_N` | `100` | How many chart entries to take, 1 to 100 |
| `STEAM_RATE_PER_SECOND` | `1.0` | Token bucket refill rate (requests per second) |
| `STEAM_BURST` | `10.0` | Token bucket capacity, at least 1 |
| `STEAM_HTTP_TIMEOUT_SECONDS` | `10.0` | Timeout of one HTTP request |
| `STEAM_MAX_ATTEMPTS` | `4` | Attempts per request, at least 1 |
| `STEAM_BACKOFF_BASE_SECONDS` | `1.0` | First retry delay, doubled each retry |
| `STEAM_BACKOFF_MAX_SECONDS` | `30.0` | Upper bound of a retry delay |

Not covered: storage, aggregates and the HTTP API.
