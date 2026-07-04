# Phase 6: Gold Analytics

## Objective
Create the business-facing tables for dashboard views, anomaly explanations, irrigation risk, and environmental stability.

## Required Inputs From Previous Phases
- `silver.sensor_readings` from Phase 5
- `shared/config/anomaly_thresholds.yaml` from Phase 1
- `shared/config/greenhouses.yaml` from Phase 1
- anomaly markers originally generated in Phase 2 and preserved through Phases 3 to 5

## Exact Files This Phase May Create Or Extend
- `pipeline/gold/gold_analytics.py`
- `pipeline/gold/README.md`
- `shared/data/gold_tables.md`
- `shared/data/analytics_rules.md`

## Non-Negotiable Reuse Rules
- Read only from `silver.sensor_readings`.
- Use the shared threshold file for rule definitions.
- Use the shared greenhouse topology when groupings or rollups need greenhouse and zone context.
- Do not create alternate risk logic in the backend or frontend if it belongs here in gold.

## Gold Table Contracts
Create exactly these tables:
- `gold.zone_hourly_metrics`
- `gold.greenhouse_daily_summary`
- `gold.anomaly_events`
- `gold.irrigation_risk`
- `gold.environmental_stability`

These are the canonical downstream analytical tables. Later phases must query these instead of recomputing analytics in application code.

## Functional Requirements
- Build greenhouse and zone rollups from silver.
- Implement deterministic, explainable anomaly detection using the shared threshold config.
- Produce explicit anomaly records that remain traceable to underlying sensor data.
- Compute irrigation risk from moisture and related context.
- Compute environmental stability from multi-metric drift behavior.

## What This Phase Must Not Do
- Do not put business logic into the backend that belongs in gold tables.
- Do not compute application-specific visual formatting here.
- Do not add black-box ML models.

## Exit Artifacts
- working gold analytics job under `pipeline/gold/gold_analytics.py`
- gold table documentation in `shared/data/gold_tables.md`
- analytics rule documentation in `shared/data/analytics_rules.md`

## Verification
- Run analytics against seeded anomaly data from the simulator.
- Confirm injected anomalies surface in `gold.anomaly_events`.
- Confirm dashboard-facing aggregates exist in the named gold tables.
- Confirm the rules used can be traced back to the shared threshold config.

## Handoff To Phase 7
Phase 7 must query the named gold tables and must not reimplement analytics logic in API code.

Phase 7 should only expose and format existing gold outputs.
