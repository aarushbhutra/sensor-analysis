# Greenhouse Sensor Analytics Platform

This repository builds a greenhouse telemetry platform on `Databricks`, `Spark`, `Kafka`, and `AWS`. The architecture source of truth is [PLAN.MD](C:\Users\aarus\OneDrive\Documents\sensor%20analysis\PLAN.MD), and the execution order lives in [PLANNING/ROADMAP.md](C:\Users\aarus\OneDrive\Documents\sensor%20analysis\PLANNING\ROADMAP.md).

## Phase Order
1. Phase 1: project skeleton and shared contracts
2. Phase 2: deterministic greenhouse sensor simulator
3. Phase 3: Kafka producer wiring and raw topic flow
4. Phase 4: bronze Spark ingest pipeline
5. Phase 5: silver normalization pipeline
6. Phase 6: gold analytics and anomaly outputs
7. Phase 7: read-only backend API
8. Phase 8: dashboard
9. Phase 9: grounded AI copilot
10. Phase 10: hardening and demo

## Top-Level Layout
- `simulator/`: synthetic greenhouse telemetry generator and anomaly injection
- `pipeline/bronze/`: Spark bronze ingest job from Kafka to Delta
- `pipeline/silver/`: Spark silver transforms for typed, deduped readings
- `pipeline/gold/`: Spark gold analytics, anomaly, and risk outputs
- `backend/`: read-only API and AI tool surface
- `dashboard/`: frontend app for metrics, anomalies, and pipeline status
- `shared/schemas/`: canonical shared contracts, including `sensor_event.schema.json`
- `shared/config/`: canonical shared config, including anomaly thresholds and greenhouse topology
- `shared/data/`: local sample artifacts or exported data needed across phases
- `PLANNING/`: roadmap and per-phase execution contracts

## Canonical Shared Contracts
Later phases must reuse these exact files instead of defining copies elsewhere:
- `shared/schemas/sensor_event.schema.json`
- `shared/config/anomaly_thresholds.yaml`
- `shared/config/greenhouses.yaml`

## Working Rules
- Keep the project centered on greenhouse telemetry.
- Keep `Spark` as the main compute engine.
- Keep `Kafka` in the ingest path.
- Keep `Databricks + AWS` as the target platform.
- Keep the AI layer read-only and grounded in query results.
