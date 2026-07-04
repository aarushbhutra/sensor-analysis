# AGENTS.md

## Purpose
This repository is for a greenhouse sensor analytics platform built on `Databricks`, `Spark`, `Kafka`, and `AWS`. The source of truth for product scope and architecture is [`PLAN.MD`](./PLAN.MD).

## Project Rules
- Keep the project centered on greenhouse telemetry, not generic IoT.
- Keep `Spark` as the main distributed compute engine.
- Keep `Kafka` in the design for streaming ingest unless the user explicitly removes it.
- Keep `Databricks + AWS` as the primary deployment target.
- Treat `S3 + Delta Lake` as the system of record.
- Keep the AI layer read-only in v1 and grounded in query results.
- Prefer explainable anomaly detection over black-box ML in v1.

## Data Domain
Core greenhouse metrics:
- temperature
- humidity
- soil moisture
- CO2
- light
- battery / device health

Operational concepts:
- greenhouse
- zone
- sensor
- anomaly
- irrigation risk
- environmental stability

## Delivery Priorities
When implementing, prioritize in this order:
1. Sensor simulation and event schema
2. Kafka/MSK ingest path
3. Databricks Spark pipeline with bronze/silver/gold tables
4. Analytics and anomaly detection
5. Backend serving layer
6. Dashboard
7. AI copilot

## Implementation Notes
- Prefer small, portfolio-friendly architecture over unnecessary enterprise sprawl.
- Keep pipelines reproducible and demoable with 1M+ synthetic events.
- Build deterministic anomaly injections so the dashboard and AI always have good demo cases.
- Preserve clear naming around greenhouses, zones, sensors, and metric families.
- Do not shift the project toward Hadoop-first language unless the user explicitly asks for that change.

## Working Agreement for Future Agents
- Read [`PLAN.MD`](./PLAN.MD) before making major architecture changes.
- If implementation diverges from the plan, update the plan in the same change set.
- Keep edits scoped and consistent with the current architecture.
- If a tradeoff is unclear, prefer the option that strengthens the Databricks/Spark/Kafka/AWS portfolio story.
