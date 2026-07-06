# Bronze Table

## Table

`bronze.sensor_events`

## Purpose

Append-only raw landing table for greenhouse telemetry read from Kafka. Bronze preserves the Phase 3 event payload as the source of truth and adds only ingestion lineage.

## Storage

- Delta path: `s3://sensor-data-lake-dev-859037107576/delta/bronze/sensor_events/`
- Checkpoint path: `s3://sensor-data-lake-dev-859037107576/checkpoints/bronze_ingest/`

## Columns

| Column | Type | Source |
|---|---|---|
| `raw_payload` | string | Kafka message value cast to JSON text |
| `kafka_topic` | string | Kafka metadata |
| `kafka_partition` | integer | Kafka metadata |
| `kafka_offset` | long | Kafka metadata |
| `ingest_ts` | timestamp | Databricks ingest time |
| `source_event_ts` | timestamp | `event_ts` extracted from `raw_payload` |

## Rules

- Do not dedupe in bronze.
- Do not normalize units in bronze.
- Do not compute anomaly scores in bronze.
- Do not drop fields from `raw_payload`.
- Downstream silver jobs must read this table instead of reading Kafka directly.
