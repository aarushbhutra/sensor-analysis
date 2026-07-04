# Phase 9: AI Copilot

## Objective
Add a grounded read-only AI layer that uses the existing backend and dashboard instead of creating parallel infrastructure.

## Required Inputs From Previous Phases
- backend app and query layer from Phase 7
- dashboard UI from Phase 8
- gold tables and anomaly outputs from Phase 6

## Exact Files This Phase May Create Or Extend
- `backend/app/agent.py`
- `backend/app/routes.py`
- `backend/app/queries.py`
- files under `dashboard/` for the chat panel
- `shared/data/ai_tools.md`

If the backend or dashboard already contains suitable files, extend them in place.

## Non-Negotiable Reuse Rules
- Reuse the existing backend service from Phase 7.
- Reuse the existing query layer from Phase 7.
- Reuse the existing dashboard UI from Phase 8.
- Reuse the existing gold tables from Phase 6.
- Do not create a separate AI microservice unless the user explicitly asks for one.

## Required Tool Surface
- `get_zone_history(zone_id, start_time, end_time)`
- `get_greenhouse_summary(greenhouse_id, start_time, end_time)`
- `get_recent_anomalies(severity, start_time, end_time)`
- `get_top_risk_zones(limit)`
- `get_pipeline_status()`

## Required Endpoint
- `POST /agent/chat`

## Functional Requirements
- AI answers must be grounded in query results.
- The response path must be read-only.
- The chat panel must live inside the existing dashboard, not a separate app.
- Missing or empty data responses must fail safely and clearly.

## What This Phase Must Not Do
- Do not allow writes to Delta tables, Kafka, or infrastructure.
- Do not let the model invent data not present in query results.
- Do not create duplicate query functions if equivalent ones already exist in the backend.

## Exit Artifacts
- AI route and tooling in the existing backend
- chat panel integrated into the existing dashboard
- tool documentation in `shared/data/ai_tools.md`

## Verification
- Ask bounded questions against seeded anomaly data.
- Confirm the answers can be traced to returned query results.
- Confirm the system refuses or safely handles requests that require unavailable data.

## Handoff To Phase 10
Phase 10 must treat this AI layer as a read-only consumer of the already-built system and validate it as part of the demo flow.
