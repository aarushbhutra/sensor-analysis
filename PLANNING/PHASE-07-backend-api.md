# Phase 7: Backend API

## Objective
Expose the existing gold outputs through a read-only API for the dashboard and later AI copilot.

## Required Inputs From Previous Phases
- gold tables from Phase 6
- job health or status notes from earlier pipeline phases
- shared schema and topology files from Phase 1 where response metadata needs consistent naming

## Exact Files This Phase May Create Or Extend
- `backend/app/main.py`
- `backend/app/routes.py`
- `backend/app/queries.py`
- `backend/app/schemas.py`
- `backend/README.md`
- `shared/data/api_contract.md`

If the backend already has this structure, extend it in place instead of creating `api/`, `service/`, or `backend_v2/`.

## Non-Negotiable Reuse Rules
- Query the gold tables created in Phase 6.
- Use existing pipeline outputs for status where available.
- Reuse shared naming and IDs from earlier phases.
- Do not compute anomalies, risk, or rollups inside the API if the gold layer already provides them.

## Required Endpoints
- `GET /metrics/overview`
- `GET /greenhouses/{greenhouse_id}/summary`
- `GET /zones/{zone_id}/history`
- `GET /anomalies`
- `GET /pipeline/status`

`POST /agent/chat` belongs to Phase 9, not this phase.

## Functional Requirements
- Return stable JSON response shapes.
- Keep the API read-only.
- Use parameterized queries or equivalent safe query construction.
- Normalize response payloads for frontend use, but do not recalculate the analytics themselves.

## What This Phase Must Not Do
- Do not build chat orchestration yet.
- Do not create a second data-serving store unless the user explicitly asks for it.
- Do not bypass gold tables to query bronze or silver for dashboard endpoints unless a specific endpoint truly requires lower-level detail.

## Exit Artifacts
- working backend app under `backend/app/`
- API documentation in `shared/data/api_contract.md`
- `backend/README.md` with local run instructions

## Verification
- Call each endpoint with representative sample inputs.
- Confirm responses come from gold outputs or pipeline status outputs.
- Confirm no endpoint writes data.

## Handoff To Phase 8
Phase 8 must use these API endpoints instead of querying Databricks or gold tables directly from the frontend.

## Handoff To Phase 9
Phase 9 must add AI tooling on top of this backend rather than creating a second parallel service.
