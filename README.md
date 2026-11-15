# gamedev-trends

Trend radar for indie game developers.

Status: Grafana dashboards, the measurement contract, the event bus (Kafka and in-memory), the Steam
collector and the ClickHouse storage exist. There is no API yet.

## Development

```bash
uv sync
cp .env.example .env
docker compose up -d --wait   # Kafka, ClickHouse, Redis, Grafana
uv run pytest                 # unit tests, no Docker needed
uv run pytest -m integration  # needs the compose stack (Kafka on localhost:9092, Redis on localhost:6379,
                              # ClickHouse on localhost:8123)
docker compose down
```

## Dashboards

Grafana runs over ClickHouse and is configured entirely by files in `grafana/`. Open
`http://localhost:3000`: the "Steam radar" dashboard (folder Radar) opens without a login, as an
anonymous Viewer. The admin user is `admin`, with the password `GRAFANA_ADMIN_PASSWORD` from `.env`. An
`.env` that already exists needs this line added (see `.env.example`).

Panels:

- "Players online": current players per game over the dashboard time range (default 7 days), one point
  per hour.
- "Chart rank": place in the Steam most-played chart over the time range. The chart is a daily summary,
  so this is updated once a day. Place 1 is at the bottom of the axis.
- "Chart movements": who rose, fell, entered or left the chart. It compares the latest chart with the
  newest chart at least `period` days older.

Variables: `game` picks the games for the two graphs (the latest number 1 is the default), `period`
(1, 7 or 30 days) is how far back the movements table compares. Games are labelled by `appid`, not by
name; in the table an appid links to the game page in Steam. The movements table is empty until there
is a chart from `period` days before the latest one.

Changing the dashboard: edits made in the UI are not saved. Export the dashboard ("Export", "Export as
JSON"), save it as `grafana/dashboards/steam.json` with a 2-space indent, then run
`docker compose restart grafana`.

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

## Storage

The `store` service reads `radar.observations.v1`, collects events into batches (up to 5 000 events or
5 seconds after the first one), writes each batch to `radar.observations` in ClickHouse and only then
commits the Kafka offset. Delivery is at-least-once: a restart before the commit reads the batch again,
and `ReplacingMergeTree` collapses the duplicates by `event_id` (the row with the later `ingested_at`
wins). If ClickHouse is unavailable the write is retried with a growing delay (up to 30 seconds) and
the consumer does not move on. The schema is created at start with `CREATE ... IF NOT EXISTS`.

Read raw measurements **only with `FINAL`**: collapsing duplicates happens in the background, and
until a merge they are visible.

```sql
SELECT observed_at, value FROM radar.observations FINAL
WHERE source = 'steam' AND entity_id = '730' AND metric = 'players_current'
  AND observed_at >= now() - INTERVAL 1 DAY
ORDER BY observed_at
```

`radar.observations_hourly` is recomputed from `observations FINAL` every 5 minutes by a refreshable
materialized view. A measurement with a window (`window_start`) belongs to the hour of the window start,
an instant one to its own hour. Columns: `samples` is the number of distinct measurements in the hour;
`value_avg` and `value_last` are the value of the hour for gauges such as `players_current`;
`value_sum` is the total for counters over a window such as `posts` and `likes`.

Run it as a service with `docker compose up -d --wait`, or from the host (needs Kafka and ClickHouse
from the compose stack):

```bash
set -a; . ./.env; set +a; uv run python -m gamedev_trends.store
```

An invalid value makes the process exit with code 2.

| Variable | Default | Meaning |
|---|---|---|
| `CLICKHOUSE_PASSWORD` | required | Password of the ClickHouse user |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers |
| `CLICKHOUSE_URL` | `http://localhost:8123` | ClickHouse HTTP interface |
| `CLICKHOUSE_USER` | `radar` | ClickHouse user |
| `CLICKHOUSE_DATABASE` | `radar` | Database; lowercase letters, digits and `_`, up to 64 characters |
| `STORE_GROUP` | `radar-store` | Kafka consumer group |
| `STORE_BATCH_MAX_EVENTS` | `5000` | Batch size limit, 1 to 100000 |
| `STORE_BATCH_MAX_SECONDS` | `5.0` | Batch time limit after the first event |
| `STORE_HTTP_TIMEOUT_SECONDS` | `30.0` | Timeout of one ClickHouse request |
| `STORE_RETRY_BASE_SECONDS` | `1.0` | First write retry delay, doubled each retry |
| `STORE_RETRY_MAX_SECONDS` | `30.0` | Upper bound of a write retry delay |

Not covered: erasures (`radar.erasures.v1` is not read yet) and the HTTP API.
