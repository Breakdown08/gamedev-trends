# gamedev-trends

Trend radar for indie game developers.

Status: Grafana dashboards, the measurement contract, the event bus (Kafka and in-memory), the Steam collector, the Bluesky collector and the ClickHouse storage exist. There is no API yet.

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

Two dashboards: "Steam radar" (`grafana/dashboards/steam.json`) and "Bluesky radar"
(`grafana/dashboards/bluesky.json`, panels below).

Steam panels:

- "Players online": current players per game over the dashboard time range (default 7 days), one point
  per hour.
- "Chart rank": place in the Steam most-played chart over the time range. The chart is a daily summary,
  so this is updated once a day. Place 1 is at the bottom of the axis.
- "Chart movements": who rose, fell, entered or left the chart. It compares the latest chart with the
  newest chart at least `period` days older.

Bluesky radar panels: "Tag frequency" (posts per whitelisted tag per hour), "Stream rate" (Jetstream
events, posts, likes and reposts per second, one point per window) and "Top posts" (the 20 tagged posts
with the most likes and reposts in the time range, with a link to bsky.app).

Steam variables: `game` picks the games for the two graphs (the latest number 1 is the default), `period`
(1, 7 or 30 days) is how far back the movements table compares. Games are labelled by `appid`, not by
name; in the table an appid links to the game page in Steam. The movements table is empty until there
is a chart from `period` days before the latest one.

Changing a dashboard: edits made in the UI are not saved. Export the dashboard ("Export", "Export as
JSON"), save it as its file in `grafana/dashboards/` with a 2-space indent, then run
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

## Bluesky collector

The collector reads the Bluesky Jetstream (`app.bsky.feed.post`, `app.bsky.feed.like`,
`app.bsky.feed.repost`) and keeps only posts with a tag from a whitelist (`BLUESKY_TAGS`; tags, not
words). Time is the Jetstream `time_us`, never the machine clock. Events are grouped into windows
`[s, s + W)` (`W` = `BLUESKY_WINDOW_SECONDS`, 60 by default); a closed window is published to
`radar.observations.v1` and `radar.erasures.v1`, in this order, all with `source = bluesky`:

| `entity_type` / `entity_id` | `metric` | `value` |
|---|---|---|
| `stream` / `jetstream` | `events`, `posts`, `likes`, `reposts` | all parsed events, created posts, likes and reposts of the window (zeros too) |
| `post` / at-URI | `tag_<tag>` | 1, one per matching tag of a new tagged post |
| `post` / at-URI | `likes`, `reposts` | likes and reposts of a tracked post in the window (no zeros) |

A post is tracked for likes and reposts for `BLUESKY_TRACK_HOURS` (24) after it was posted. Tag frequency
is a query: the number of `tag_<tag>` rows per hour. Not stored: post texts, handles, who liked. Not
subtracted: withdrawn likes and reposts, so `likes` means "likes made in the window". Quotes, replies and
post edits are not handled.

Deletions: a deleted tagged post becomes an `Erasure` in `radar.erasures.v1`. An `account` event with
`active = false` and a status of `deleted`, `deactivated`, `takendown` or `suspended` becomes an
`Erasure` for every known post of that author (statuses such as `desynchronized` change nothing). The
storage applies them (see Storage). Known posts live in Redis, one sorted set per author, without a
time limit.

Cursor: after each published window the `time_us` of the start of the next one is saved in Redis
(`bluesky:cursor`, with AOF persistence in the `redis-data` volume). After a reconnect or restart the
Jetstream is read again from the cursor and the open window is recomputed whole; the measurements get
the same `event_id`, so no duplicates remain. The first window after a cold start (no cursor) is
incomplete.

Back pressure: between the socket and Kafka there is a bounded queue (`BLUESKY_QUEUE_SIZE`). When it is
full the socket is not read. A lost connection or `BLUESKY_IDLE_TIMEOUT_SECONDS` without frames starts a
new session, alternating between the endpoints, with a delay that grows up to
`BLUESKY_BACKOFF_MAX_SECONDS`.

Every `BLUESKY_STATS_SECONDS` the log has a line like
`bluesky stats: rate=235.0/s events=14100 posts=... likes=... reposts=... tagged_posts=... engagements=... erasures=... late=... malformed=... queue=0/10000`
(counts since the previous line). The same rates go to ClickHouse as the `stream` rows.

Run from the host (needs the compose stack for Kafka and Redis; the process stops after the duration and
prints a `bluesky summary:` line):

```bash
docker compose up -d --wait
uv run python -m gamedev_trends.bluesky --duration 600
```

On NixOS the uv-managed Python does not find the system CA bundle (`/etc/ssl/cert.pem` does not exist and
`/etc/ssl/certs` has no hashed links), so every Jetstream connection fails with
`SSLCertVerificationError`. Point OpenSSL at the bundle:

```bash
SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt uv run python -m gamedev_trends.bluesky --duration 600
```

Run it as a service. The service is in the `collectors` profile, so plain `docker compose up -d --wait`
does not start it:

```bash
docker compose --profile collectors up -d --wait
```

An invalid value makes the process exit with code 2.

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis for the cursor and the post index |
| `BLUESKY_ENDPOINTS` | the two public Jetstream hosts | Comma-separated `wss://` or `ws://` URLs without a query |
| `BLUESKY_TAGS` | `gamedev,indiedev,indiegame,godot,pixelart,screenshotsaturday,mobilegame` | Tag whitelist, `[a-z][a-z0-9_]*`, up to 28 characters |
| `BLUESKY_WINDOW_SECONDS` | `60` | Window length, at least 10 and a divisor of 3600 |
| `BLUESKY_TRACK_HOURS` | `24` | How long likes and reposts of a post count, 1 to 168 |
| `BLUESKY_QUEUE_SIZE` | `10000` | Frames between the socket and the handler, 1 to 1000000 |
| `BLUESKY_IDLE_TIMEOUT_SECONDS` | `60` | Reconnect after this long without frames |
| `BLUESKY_STATS_SECONDS` | `60` | Interval of the `bluesky stats` line |
| `BLUESKY_BACKOFF_BASE_SECONDS` | `1` | First reconnect delay, doubled after each failed session |
| `BLUESKY_BACKOFF_MAX_SECONDS` | `30` | Upper bound of the reconnect delay |

Redis memory: the post index has no expiry. An estimate, not measured: 4 to 5 thousand tagged posts a
day, about 200 MB a year.

### Measured

- 2026-11-16, 08:48–08:58 UTC, from the host, default tags: `bluesky summary` gave 121 947 events in
  605 s (201.6/s), 7 tagged posts, 13 likes and reposts of tracked posts, 0 erasures. ClickHouse has 10
  consecutive `stream events` windows for that run, 195 to 209 events/s each.
- 2026-11-16, service from the image (`--profile collectors`): `restart` at 09:03 UTC resumed from the
  cursor of the open window, `stream events` windows 08:55–09:08 have no gap. Kafka stopped for 2 minutes
  (09:05–09:07): the collector logged `PublishError`, reconnected from the cursor and did not exit; the
  windows of the outage arrived after `docker compose start kafka` with the usual values (206–213/s). One
  real erasure (a deleted tagged post) left 0 observations of that URI.

## Storage

The `store` service reads `radar.observations.v1` and `radar.erasures.v1`, collects events into batches (up to 5 000 events or
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

Erasures: an `Erasure` from `radar.erasures.v1` is written to `radar.erasures` and deletes the
measurements of its entity (exact match of source, type and id) from `radar.observations`. A measurement
of an erased entity that arrives later is dropped before it is written. The hourly aggregate drops the
erased rows at its next recomputation, up to 5 minutes later. The `radar-store` consumer group starts
reading the erasures topic from the beginning.

Not covered: the HTTP API.
