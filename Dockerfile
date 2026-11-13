FROM python:3.12-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY scripts ./scripts

ENV EVENTS_PARQUET=/data/events.parquet \
    APP_DATA_DIR=/appdata \
    MAX_CONCURRENT_QUERIES=2 \
    QUERY_TIMEOUT_SECONDS=10

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
