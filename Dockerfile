FROM python:3.12.11-slim-bookworm AS runtime-base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 agentos \
    && useradd --uid 10001 --gid agentos --create-home --shell /usr/sbin/nologin agentos

COPY requirements.txt ./requirements.txt
RUN python -m pip install --upgrade "pip==25.1.1" \
    && python -m pip install -r requirements.txt

COPY pyproject.toml README.md ./
COPY agentos ./agentos
RUN python -m pip install --no-deps . \
    && mkdir -p /workspaces /home/agentos/.cache \
    && chown -R agentos:agentos /app /workspaces /home/agentos

USER agentos

ENTRYPOINT ["agentos"]

FROM runtime-base AS test

USER root
COPY requirements-dev.txt ./requirements-dev.txt
RUN python -m pip install ".[test]"
USER agentos

FROM runtime-base AS runtime

USER root
RUN rm -rf /app/agentos/tests
USER agentos
