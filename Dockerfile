FROM ghcr.io/astral-sh/uv:0.12.11 AS uv
FROM python:3.14.7-slim-trixie
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN uv sync --locked --no-dev
USER nobody
CMD ["/app/.venv/bin/python", "-m", "gamedev_trends.steam"]
