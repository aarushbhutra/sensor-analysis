# Phase 8: Dashboard

## Objective
Build the greenhouse dashboard using the existing backend API as the only application data source.

## Required Inputs From Previous Phases
- backend endpoints from Phase 7
- gold-derived response shapes from the API
- greenhouse IDs, zone IDs, and metric naming already standardized in earlier phases

## Exact Files This Phase May Create Or Extend
- files under `dashboard/`
- `dashboard/README.md`
- `shared/data/dashboard_views.md`

Do not create a second frontend root like `frontend2/` or `webapp/` if `dashboard/` already exists.

## Non-Negotiable Reuse Rules
- Use the backend API from Phase 7 as the only data source.
- Reuse existing greenhouse, zone, anomaly, and metric naming.
- Reuse the gold-derived API outputs; do not re-derive analytics in the UI.

## Required Views
- overview page
- greenhouse detail page
- zone history page or panel
- anomaly feed
- pipeline status panel

## Required Filters
- time range
- greenhouse
- zone
- severity where relevant

## Functional Requirements
- Show ingest/health summary on the overview.
- Let a reviewer move from global overview to one greenhouse to one zone.
- Render anomaly evidence coming from API responses.
- Render pipeline status coming from the API.

## What This Phase Must Not Do
- Do not call Databricks directly from the frontend.
- Do not create business logic duplicates that disagree with gold tables.
- Do not add AI chat in this phase.

## Exit Artifacts
- working dashboard code under `dashboard/`
- `dashboard/README.md`
- view documentation in `shared/data/dashboard_views.md`

## Verification
- Run the dashboard against the real backend.
- Confirm all views use live API data, not mocks.
- Confirm a seeded anomaly from earlier phases is visible in the UI.

## Handoff To Phase 9
Phase 9 must attach the AI chat panel to this existing dashboard instead of building a separate chat-only UI.
