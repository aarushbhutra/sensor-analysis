# Phase 1: Project Skeleton

## Objective
Create the canonical repo structure, shared contracts, and baseline documentation that every later phase must reuse.

## Why This Phase Exists
Without a fixed structure, later phases will create duplicate schemas, duplicate config files, and duplicate entrypoints. This phase removes that ambiguity before implementation starts.

## Must Read Before Work
- [PLAN.MD](../PLAN.MD)
- [INFRASTRUCTURE.md](../INFRASTRUCTURE.md)
- [ROADMAP.md](./ROADMAP.md)

## Exact Repo Structure To Create
Create these directories if they do not already exist:

- `simulator/src/`
- `simulator/output/`
- `pipeline/bronze/`
- `pipeline/silver/`
- `pipeline/gold/`
- `backend/app/`
- `dashboard/`
- `shared/schemas/`
- `shared/config/`
- `shared/data/`

Create these initial files if they do not already exist:

- `README.md`
- `.env.example`
- `shared/schemas/sensor_event.schema.json`
- `shared/config/anomaly_thresholds.yaml`
- `shared/config/greenhouses.yaml`

## Non-Negotiable Contracts Established In This Phase
- `shared/schemas/sensor_event.schema.json` is the only canonical event schema file.
- `shared/config/anomaly_thresholds.yaml` is the only canonical anomaly-threshold config file.
- `shared/config/greenhouses.yaml` is the only canonical greenhouse topology config file.
- Later phases must import or reference these exact files instead of defining their own copies.

## What This Phase Must Contain
- A clear folder layout matching the architecture:
  - simulator code goes under `simulator/`
  - Spark pipeline code goes under `pipeline/`
  - backend API code goes under `backend/`
  - frontend code goes under `dashboard/`
  - shared contracts go under `shared/`
- `README.md` must explain the order of phases and what each top-level folder is for.
- `.env.example` must name the future environment variables even if values are placeholders.
- `sensor_event.schema.json` must define the fields already named in `PLAN.MD` and `INFRASTRUCTURE.md`.
- `anomaly_thresholds.yaml` must define v1 thresholds for temperature, humidity, soil moisture, CO2, light, battery, and missing-data timing.
- `greenhouses.yaml` must define the baseline greenhouse topology with greenhouse, zone, and sensor relationships.

## What This Phase Must Not Do
- Do not implement the simulator logic yet.
- Do not add Kafka client code yet.
- Do not add Spark job code yet.
- Do not add API routes yet.
- Do not add frontend code yet.

## Reuse Rules For Later Phases
- Phase 2 must reuse `shared/schemas/sensor_event.schema.json`, `shared/config/anomaly_thresholds.yaml`, and `shared/config/greenhouses.yaml`.
- Phase 3 must reuse the simulator entrypoint created in Phase 2 and the same schema from this phase.
- Phases 4 through 10 must keep using the same schema and config files unless the user explicitly asks to migrate them.

## Exit Artifacts
This phase is complete only if these artifacts exist:
- `README.md`
- `.env.example`
- `shared/schemas/sensor_event.schema.json`
- `shared/config/anomaly_thresholds.yaml`
- `shared/config/greenhouses.yaml`
- all canonical directories listed above

## Verification
- Confirm the repo contains the exact canonical folders listed above.
- Confirm there is exactly one event schema file under `shared/schemas/`.
- Confirm there is exactly one threshold config file under `shared/config/`.
- Confirm `README.md` points later work to those shared files.

## Handoff To Phase 2
Phase 2 must treat these files as source of truth:
- `shared/schemas/sensor_event.schema.json`
- `shared/config/anomaly_thresholds.yaml`
- `shared/config/greenhouses.yaml`

Phase 2 must not create any second schema or second threshold config.
