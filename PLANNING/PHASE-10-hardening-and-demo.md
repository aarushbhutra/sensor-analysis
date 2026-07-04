# Phase 10: Hardening And Demo

## Objective
Validate the full system, tighten the obvious weak points, and package the project for a repeatable demo and portfolio walkthrough.

## Required Inputs From Previous Phases
- simulator from Phase 2
- Kafka flow from Phase 3
- bronze pipeline from Phase 4
- silver pipeline from Phase 5
- gold analytics from Phase 6
- backend API from Phase 7
- dashboard from Phase 8
- AI copilot from Phase 9

If any of those are missing, this phase should fix the missing prerequisite before calling the system demo-ready.

## Exact Files This Phase May Create Or Extend
- `shared/data/system_validation.md`
- `shared/data/demo_script.md`
- `shared/data/operational_runbook.md`
- existing README files where run instructions need tightening

## Non-Negotiable Reuse Rules
- Validate the existing end-to-end system.
- Fix obvious bottlenecks in place before introducing new services.
- Reuse the existing simulator, pipelines, backend, dashboard, and AI layer.
- Do not introduce extra infra tiers unless a measured bottleneck proves the need.

## Functional Requirements
- Run sustained simulator load.
- Validate the normal continuous-streaming path with the full `100`-sensor topology.
- Validate the 1M+ event target path using the faster load-test cadence rather than by inflating sensor count.
- Validate bronze, silver, and gold freshness.
- Validate dashboard behavior under real system output.
- Validate AI answers against seeded anomalies and current pipeline status.
- Prepare a repeatable walkthrough from simulator start to anomaly explanation.

## What This Phase Must Not Do
- Do not redesign the architecture unless a hard blocker is proven.
- Do not add Kubernetes, RDS, or a second serving tier just because they sound more complete.
- Do not rewrite earlier phases instead of validating them unless they are actually broken.

## Exit Artifacts
- validation notes in `shared/data/system_validation.md`
- demo script in `shared/data/demo_script.md`
- operational notes in `shared/data/operational_runbook.md`

## Verification
- Run the end-to-end path from simulator to dashboard.
- Confirm seeded anomalies survive every stage.
- Confirm API and dashboard responses are stable.
- Confirm AI answers remain grounded.
- Capture failures, bottlenecks, and recovery steps in the runbook.

## Final Handoff
At the end of this phase, the repo should support:
- a reproducible demo flow
- a portfolio walkthrough tied to the real architecture
- a clear explanation of what exists, what was validated, and what is intentionally deferred
