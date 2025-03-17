FROM ghcr.io/astral-sh/uv:python3.13-bookworm AS builder

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

ADD . /app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

FROM python:3.13-bookworm

COPY --from=builder --chown=app:app /app /app

ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app

EXPOSE 8000
RUN ./manage.py collectstatic --noinput
CMD ["gunicorn", "--bind", ":8000", "--workers", "3", "--access-logfile", "-", "--log-file", "-", "hmmerapi.wsgi"]