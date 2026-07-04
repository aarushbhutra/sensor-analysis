# Phase 2: Sensor Simulator

## Objective
Build the deterministic greenhouse event generator that produces schema-valid telemetry and controlled anomaly scenarios.

## Required Inputs From Previous Phases
- `shared/schemas/sensor_event.schema.json` from Phase 1
- `shared/config/anomaly_thresholds.yaml` from Phase 1
- `shared/config/greenhouses.yaml` from Phase 1

If any of those files do not exist, Phase 2 is blocked and Phase 1 must be finished first.

## Exact Files This Phase May Create Or Extend
- `simulator/src/models.py`
- `simulator/src/generator.py`
- `simulator/src/anomalies.py`
- `simulator/src/main.py`
- `simulator/output/sample_events.jsonl`
- `simulator/README.md`

If the repo already has equivalent files under `simulator/src/`, extend them in place instead of creating alternates.

## Non-Negotiable Reuse Rules
- Read the event contract from `shared/schemas/sensor_event.schema.json`.
- Read threshold values from `shared/config/anomaly_thresholds.yaml`.
- Read greenhouse and zone topology from `shared/config/greenhouses.yaml`.
- Do not create `simulator/schema.json`, `simulator/config.yaml`, or any other duplicate contract file if the shared file already exists.

## Functional Requirements
- Reuse the `100`-sensor topology from `shared/config/greenhouses.yaml`.
- Generate telemetry for:
  - temperature
  - humidity
  - soil moisture
  - CO2
  - light
  - battery
- Model the hierarchy:
  - greenhouse
  - zone
  - sensor
- Emit one full reading snapshot per event.
- Support deterministic generation from a fixed seed or fixed configuration.
- Support continuous streaming instead of hourly-only generation.
- Support at least two rates:
  - normal demo mode at one event every `15` seconds per sensor
  - load-test mode at one event every `1` second per sensor
- Support deterministic anomaly injection for at least:
  - temperature band breach
  - humidity drift
  - low soil moisture
  - CO2 range breach
  - light anomaly
  - battery degradation
  - missing-data or dead-sensor simulation

## Output Contract
- The simulator must write newline-delimited JSON events.
- Every event must conform to `shared/schemas/sensor_event.schema.json`.
- At least one output file must exist at `simulator/output/sample_events.jsonl`.
- Output must include explicit anomaly markers so later phases can validate that anomalies survive the pipeline.

## What This Phase Must Not Do
- Do not publish to Kafka yet.
- Do not add ECS deployment code yet.
- Do not move schema/config ownership away from `shared/`.

## Exit Artifacts
- working simulator code under `simulator/src/`
- `simulator/output/sample_events.jsonl`
- `simulator/README.md` with run instructions

## Verification
- Run the simulator locally.
- Confirm output is reproducible with the same seed/config.
- Confirm the sample output matches the shared schema.
- Confirm anomaly windows exist in the sample output and are identifiable by field values and markers.

## Handoff To Phase 3
Phase 3 must reuse:
- the simulator entrypoint from this phase
- the sample event shape from this phase
- the shared schema from Phase 1

Phase 3 must not create a second standalone producer that bypasses the simulator code unless the user explicitly asks for a replacement.

## Completion Notes
- Status: Complete
- Entrypoint: `python simulator/src/main.py`
- Sample output: `simulator/output/sample_events.jsonl`
- Verification run: `python simulator/src/main.py --events 300 --seed 42 --mode normal --check`
- Additional checks confirmed deterministic repeat output, 100 unique sensors, four greenhouses, all required anomaly markers, and load mode one-second cadence.
