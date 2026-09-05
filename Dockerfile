# One-click UrbanFlow: Java + Python + deps baked in, pipeline runs itself,
# dashboard comes up ready. `docker compose up --build` is the whole setup.
FROM eclipse-temurin:21-jdk-jammy

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-venv python3-pip make curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencies first so they cache across code-only changes.
COPY requirements.txt Makefile ./
RUN python3 -m venv .venv \
    && .venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel \
    && .venv/bin/pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY schema ./schema
COPY scripts ./scripts
COPY tests ./tests
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# TIER=0 real data on first run; set INGEST=0 for synthetic-only (no network).
ENV INGEST=1
ENV TIER=0

# `make dash` just runs `streamlit run` with no flags — these env vars make it
# bind to all interfaces so the port mapping to the host actually works.
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_SERVER_HEADLESS=true

EXPOSE 8501
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["dash"]
