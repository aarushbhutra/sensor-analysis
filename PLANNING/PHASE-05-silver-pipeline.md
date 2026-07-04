# Phase 5: Silver Pipeline

## Objective
Transform the existing bronze layer into typed, normalized, deduped sensor records that the analytics layer can trust.

## Required Inputs From Previous Phases
- `bronze.sensor_events` from Phase 4
- `shared/schemas/sensor_event.schema.json` from Phase 1
- `shared/config/anomaly_thresholds.yaml` from Phase 1
- greenhouse topology from `shared/config/greenhouses.yaml` from Phase 1

## Exact Files This Phase May Create Or Extend
- `pipeline/silver/silver_transform.py`
- `pipeline/silver/README.md`
- `shared/data/silver_table.md`
- `shared/data/data_quality_rules.md`

## Non-Negotiable Reuse Rules
- Read only from `bronze.sensor_events` for raw pipeline input.
- Parse against the existing shared event schema.
- Reuse the shared topology and thresholds where they are needed for validation or derived fields.
- Do not introduce a second cleaned table if `silver.sensor_readings` already exists.

## Silver Table Contract
- Table name: `silver.sensor_readings`
- Must include typed forms of the event fields
- Must include normalized timestamps
- Must include dedupe logic
- Must include derived partition columns like date and hour
- Must include explicit flags for malformed records or missing-data conditions if the design keeps those rows

## Functional Requirements
- Parse bronze JSON payloads into typed columns.
- Validate required fields.
- Drop or quarantine malformed rows in a documented way.
- Remove duplicate events in a documented way.
- Standardize categorical status fields.
- Preserve enough lineage to trace a silver row back to the bronze event if needed.

## What This Phase Must Not Do
- Do not calculate dashboard-ready aggregates yet.
- Do not calculate final anomaly tables yet.
- Do not bypass bronze by reading directly from Kafka.

## Exit Artifacts
- working silver transform job under `pipeline/silver/silver_transform.py`
- silver table documentation in `shared/data/silver_table.md`
- quality rule documentation in `shared/data/data_quality_rules.md`

## Verification
- Run the transform against known-good bronze data.
- Run the transform against known malformed or duplicate cases.
- Confirm the output schema is typed and queryable.
- Confirm dedupe behavior is deterministic and documented.

## Handoff To Phase 6
Phase 6 must read from `silver.sensor_readings` and use it as the only analytical input layer.

Phase 6 must not recompute parsing and dedupe logic from bronze if silver already exists.
