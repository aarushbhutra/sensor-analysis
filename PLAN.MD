# Greenhouse Sensor Analytics Platform with Databricks, Spark, Kafka, and AWS

## Summary
Build a cloud-based greenhouse analytics platform that simulates and processes 1M+ sensor datapoints using distributed streaming ingest, Spark-based analytics, and an AI copilot for grounded analysis. The project should showcase modern data engineering skill through `Databricks`, `Apache Spark`, `Kafka`, `AWS`, and a bronze/silver/gold lakehouse design, while keeping the sensor fleet small enough to demo cheaply.

Primary portfolio story:
- Built an end-to-end streaming and analytics platform for 100 continuously streaming greenhouse sensors and 1M+ greenhouse sensor events on AWS
- Used `Kafka` for ingestion, `Databricks Spark` for distributed ETL and anomaly detection, and `S3 + Delta Lake` for the analytical data lake
- Added a web dashboard and AI copilot for natural-language analysis over processed greenhouse metrics

## Final Architecture
Use these components and roles:

- `Sensor simulators`
  - Python producers generate synthetic greenhouse telemetry from many virtual sensors
  - Run locally, on EC2, or in ECS tasks to simulate multiple devices and greenhouse zones

- `Kafka`
  - Use `Amazon MSK` as the event backbone
  - Receives real-time greenhouse events from all producers
  - Buffers and decouples producers from downstream processing
  - Makes the system a real streaming architecture rather than a file upload demo

- `Spark`
  - Use `Databricks` with `Spark Structured Streaming` for ingestion from Kafka
  - Use Spark batch jobs and streaming jobs for all heavy processing
  - Spark is the main distributed compute engine in this project

- `AWS storage`
  - Use `Amazon S3` as the system of record
  - Store bronze, silver, and gold Delta tables on S3

- `Delta Lake / Databricks`
  - Bronze: raw greenhouse events from Kafka
  - Silver: cleaned, typed, quality-checked greenhouse records
  - Gold: aggregates, anomaly tables, zone health scores, irrigation risk summaries, and yield-support summaries

- `Catalog / schema management`
  - Use `Databricks Unity Catalog` if available
  - Otherwise use `AWS Glue Data Catalog`

- `Serving/API`
  - Backend service queries gold tables or mirrored serving tables
  - Exposes metrics, history, anomalies, and AI tools
  - Optional `RDS PostgreSQL` for low-latency serving if dashboard queries need faster reads

- `Frontend`
  - Web dashboard for greenhouse charts, pipeline status, anomaly feed, and AI chat

- `AI copilot`
  - Read-only assistant
  - Uses backend query tools over Spark/Delta outputs
  - Answers questions, summarizes anomalies, compares greenhouse zones and time windows, explains alerts, and suggests investigation paths

## Why Each Technology Is Used
- `Kafka/MSK`
  - Real-time event ingestion from multiple distributed producers
  - Handles scale, buffering, and clean decoupling between sensor generation and analytics

- `Spark`
  - Core distributed analytics engine
  - Performs streaming ingest, ETL, aggregations, anomaly detection, and feature generation
  - This is where most of the big-data skill is demonstrated

- `Databricks`
  - Managed Spark environment
  - Simplifies cluster setup, notebooks, jobs, Delta Lake, and monitoring
  - Keeps the project modern and resume-relevant

- `AWS`
  - Provides cloud infrastructure and managed services
  - `S3` for durable lake storage, `MSK` for Kafka, optional `EC2/ECS` for producers, optional `RDS` for serving

## Greenhouse Data Model
Sensor event schema:
- `event_id`
- `event_ts`
- `sensor_id`
- `greenhouse_id`
- `zone_id`
- `metric_family`
- `temperature_c`
- `humidity_pct`
- `soil_moisture_pct`
- `co2_ppm`
- `light_lux`
- `battery_level`
- `fan_status`
- `irrigation_status`
- `location_status`
- `is_injected_anomaly`
- `anomaly_type`

Event contract rules:
- `event_id` is unique per emitted reading
- `event_ts` is the canonical UTC event timestamp
- `metric_family` identifies the sensor family that emitted the reading
- one event contains the full sensor reading snapshot
- anomaly scenarios must be marked explicitly with `is_injected_anomaly` and `anomaly_type`

Recommended scale:
- 100 simulated sensors
- 4 greenhouses
- 5 zones per greenhouse
- 5 sensors per zone
- 1M+ total events during sustained or burst runs
- Events generated continuously over a defined test window
- Normal demo cadence: 1 event every 15 seconds per sensor
- Load-test cadence: 1 event every 1 second per sensor
- Controlled anomaly scenarios injected into selected sensors or zones

## Processing Design
Bronze layer:
- Raw Kafka event payloads
- Minimal transformation
- Append-only landing zone
- Includes ingest metadata and event timestamps

Silver layer:
- Parsed and typed records
- Remove malformed or duplicate events
- Standardize timestamps, units, and status values
- Add derived columns such as hour, day, greenhouse, zone, and operating period

Gold layer:
- Hourly aggregates by greenhouse and zone
- Daily greenhouse condition summaries
- Anomaly events
- Zone health scores
- Irrigation risk summaries
- Environmental stability scores
- Top-risk zones and sensors for dashboard and AI use

Spark jobs:
- `stream_ingest_job`
  - Reads Kafka topics with Structured Streaming
  - Writes bronze Delta tables on S3

- `silver_transform_job`
  - Cleans bronze data
  - Writes silver Delta tables

- `gold_analytics_job`
  - Computes aggregates, drift metrics, anomaly tables, zone health scores, and irrigation risk summaries
  - Runs as scheduled batch or micro-batch job

## Greenhouse Analytics Scope
Implement these anomaly types in v1:
- Temperature too high or too low for stable greenhouse operation
- Humidity drift outside the desired band
- Soil moisture too low, indicating irrigation risk
- CO2 or light readings outside expected range
- Missing-data / dead-sensor behavior
- Battery degradation
- Zone instability, where multiple metrics drift together

Keep v1 logic deterministic and explainable:
- Rolling averages
- Baseline deviation thresholds
- Simple rules and time-window checks

Avoid complex predictive ML in v1. Better to show solid data engineering and explainable analytics first.

## Backend and AI Interfaces
Backend endpoints:
- `GET /metrics/overview`
- `GET /greenhouses/{greenhouse_id}/summary`
- `GET /zones/{zone_id}/history`
- `GET /anomalies`
- `GET /pipeline/status`
- `POST /agent/chat`

AI tool functions:
- `get_zone_history(zone_id, start_time, end_time)`
- `get_greenhouse_summary(greenhouse_id, start_time, end_time)`
- `get_recent_anomalies(severity, start_time, end_time)`
- `get_top_risk_zones(limit)`
- `get_pipeline_status()`

AI behavior:
- Summarize recent greenhouse anomalies
- Explain alert causes from queried data
- Compare greenhouses, zones, or time periods
- Highlight unstable zones and irrigation risk
- Never answer from guesses; all answers must be grounded in gold tables or query outputs

## Dashboard Scope
Main views:
- Overview page with ingest rate, total events, active sensors, anomaly count, and greenhouse health
- Greenhouse detail page with zone summaries
- Zone detail page with time-series charts
- Anomaly feed with severity and explanation
- AI chat panel
- Pipeline status panel

Charts:
- Temperature
- Humidity
- Soil moisture
- CO2
- Light
- Battery trend

Filters:
- Time range
- Greenhouse
- Zone
- Sensor ID
- Severity

## Implementation Plan
Phase 1. Foundation
- Create AWS resources: S3 bucket, MSK cluster, Databricks workspace
- Define topic names, schemas, and bronze/silver/gold layout
- Build greenhouse sensor simulator
- Publish events into Kafka
- Prove Kafka-to-Databricks-to-S3 bronze ingestion

Phase 2. Core data pipeline
- Build Spark Structured Streaming job for bronze ingest
- Build Spark transformation job for silver layer
- Build gold analytics job for aggregates, zone health, and anomaly detection
- Validate the full 1M+ event pipeline

Phase 3. Product layer
- Build backend API on top of gold outputs
- Build dashboard with greenhouse charts and anomaly feed
- Build AI copilot using tool-calling over backend query functions
- Add deterministic greenhouse anomaly injections for demo scenarios

Phase 4. Hardening and demo
- Run concurrent producers
- Verify pipeline stability at 1M+ events
- Tune dashboard query path
- Prepare an architecture diagram and walkthrough
- Document design choices and tradeoffs

## Test Plan
Functional checks:
- 100 sensors generate continuous streaming telemetry and successfully ingest into Kafka
- 1M+ events generated and successfully ingested during a sustained or burst run
- Kafka topics handle concurrent producers
- Bronze, silver, and gold tables populate correctly
- Spark jobs produce expected outputs on known sample windows
- Injected anomalies show up in gold tables, dashboard, and AI responses
- AI answers remain grounded in actual query outputs

System checks:
- MSK ingest remains stable during sustained event flow
- Databricks jobs run reliably on cluster compute
- S3/Delta lake stores raw and processed data correctly
- Dashboard reads are responsive for demo scenarios
- Failure of one producer does not break the end-to-end flow

## Deliverables
Produce these artifacts:
- Architecture diagram
- Greenhouse sensor simulator code
- Kafka topics and ingestion job
- Databricks notebooks/jobs
- Bronze/silver/gold tables
- Backend API
- Dashboard
- AI copilot
- Demo script
- Short technical writeup for portfolio and resume use

## Assumptions
- Main goal is modern data engineering portfolio value
- `Spark` is the core analytics engine
- `Kafka` remains part of the final design because the project is near-real-time
- `Databricks + AWS` is the primary platform
- v1 uses synthetic greenhouse telemetry
- v1 uses a 100-sensor baseline topology with configurable event rates instead of a massive device fleet
- v1 focuses on explainable analytics and grounded AI, not automated greenhouse control
