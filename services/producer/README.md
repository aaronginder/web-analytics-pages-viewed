# Producer Service

Generates synthetic web page view events and publishes them to Kafka.

## Table of Contents

- [What it does](#what-it-does)
- [Prerequisites](#prerequisites)
- [Run with Docker Compose](#run-with-docker-compose)
- [Build the image manually](#build-the-image-manually)
- [Run the container manually](#run-the-container-manually)
- [Configuration](#configuration)
- [Development](#development)

## What it does

- Produces page_view events with user/page IDs, timestamps, and engagement metadata.
- Sends messages keyed by `user_id` to maintain per-user ordering.

## Prerequisites

- Docker
- Running Kafka broker

## Run with Docker Compose

From repo root:

```bash
docker-compose -f infrastructure/docker/docker-compose.yml up -d producer
```

The compose file builds this service and points it at `kafka:9092`.

## Build the image manually

```bash
docker build -t producer-service .
```

## Run the container manually

```bash
docker run --rm \
  -e KAFKA_BROKER_SERVERS=localhost:29092 \
  producer-service
```

## Configuration

Environment variables:

- `KAFKA_BROKER_SERVERS` (default: `kafka:9092`)
- `TOPIC_NAME` (default: `web-analytics-events`)
- `NUM_EVENTS` (default: `1000`)

Command-line overrides (when running the script directly):

- `--bootstrap_servers` (default: `localhost:29092`)
- `--topic` (default: `web-analytics-events`)
- `--num_events` (default: `1000`)

## Development

```bash
# Run locally (requires Kafka reachable at localhost:29092)
python producer.py --bootstrap_servers localhost:29092 --topic web-analytics-events --num_events 100
```
