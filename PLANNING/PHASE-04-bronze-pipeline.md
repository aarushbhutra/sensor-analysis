# Phase 4: Bronze Pipeline

## Objective
Create the Spark Structured Streaming job that reads the existing Kafka topic and lands raw events in the bronze Delta layer with minimal transformation.

## Required Inputs From Previous Phases
- topic `greenhouse.sensor-events.v1` from Phase 3
- `shared/schemas/sensor_event.schema.json` from Phase 1
- Kafka config from `shared/config/kafka.yaml` from Phase 3

## Exact Files This Phase May Create Or Extend
- `pipeline/bronze/stream_ingest.py`
- `pipeline/bronze/README.md`
- `shared/data/bronze_table.md`
- `shared/data/databricks_jobs.md`

## Non-Negotiable Reuse Rules
- Read from the existing topic `greenhouse.sensor-events.v1`.
- Preserve the event payload exactly as published by Phase 3.
- Use the existing shared schema for parsing or validation references.
- Do not create a second raw landing table if the bronze table already exists.

## Bronze Table Contract
- Table name: `bronze.sensor_events`
- Must include:
  - raw JSON payload
  - Kafka topic
  - Kafka partition
  - Kafka offset
  - ingest timestamp
  - source event timestamp if present in payload

## Functional Requirements
- Use Spark Structured Streaming.
- Read from Kafka.
- Write append-only raw records to bronze.
- Store streaming checkpoints in the bronze checkpoint location defined by the infrastructure plan.
- Keep transformation minimal. Bronze is for raw truth, not cleanup.

## What This Phase Must Not Do
- Do not dedupe records yet.
- Do not normalize units yet.
- Do not compute anomaly scores yet.
- Do not create silver or gold tables in this phase.

## Exit Artifacts
- working bronze ingest job under `pipeline/bronze/stream_ingest.py`
- bronze table documentation in `shared/data/bronze_table.md`
- Databricks job notes updated in `shared/data/databricks_jobs.md`

## Verification
- Run the streaming job against Kafka.
- Confirm records appear in `bronze.sensor_events`.
- Confirm Kafka offsets are stored.
- Confirm restarting the job resumes from checkpoints instead of starting from scratch.

## Handoff To Phase 5
Phase 5 must read from `bronze.sensor_events` and must treat that table as the only raw source for downstream cleanup.

Phase 5 must not read directly from Kafka if `bronze.sensor_events` already exists and is working.

## Completion Notes
- Status: Ready for live Databricks/MSK verification
- Entrypoint: `pipeline/bronze/stream_ingest.py`
- Databricks job: `stream_ingest_job`
- Bronze table: `bronze.sensor_events`
- Delta path: `s3://sensor-data-lake-dev-859037107576/delta/bronze/sensor_events/`
- Checkpoint path: `s3://sensor-data-lake-dev-859037107576/checkpoints/bronze_ingest/`
- Local structural verification: `$env:BRONZE_INGEST_SELF_CHECK='1'; python pipeline/bronze/stream_ingest.py`
- Live Databricks verification pending: publish to `greenhouse.sensor-events.v1`, run the `AvailableNow` job, query `bronze.sensor_events`, then publish more data and rerun the job to confirm checkpoint resume behavior.
