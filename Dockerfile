FROM ghcr.io/astral-sh/uv:python3.11-bookworm

COPY . /app

WORKDIR /app

RUN uv sync --frozen

CMD ["tail", "-f", "/dev/null"]