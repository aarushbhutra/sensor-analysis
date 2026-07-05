# Phase 3: Kafka Ingest

## Objective
Connect the existing simulator to Kafka and prove that raw sensor events flow into the canonical topic without changing the event contract.

## Required Inputs From Previous Phases
- `shared/schemas/sensor_event.schema.json` from Phase 1
- simulator code under `simulator/src/` from Phase 2
- sample output behavior from `simulator/output/sample_events.jsonl` from Phase 2

## Exact Files This Phase May Create Or Extend
- `simulator/src/kafka_producer.py`
- `simulator/src/main.py`
- `simulator/README.md`
- `shared/config/kafka.yaml`
- `shared/data/kafka_topics.md`
- `shared/data/kafka_validation.md`

## Non-Negotiable Reuse Rules
- The producer must use the existing simulator output model from Phase 2.
- The producer must publish the same event shape already defined in `shared/schemas/sensor_event.schema.json`.
- Do not create a second event schema for Kafka.
- Do not fork the simulator into a second app just to add Kafka publishing.

## Kafka Contract
- Topic name: `sensor.telemetry.raw`
- Partition key: `greenhouse_id:zone_id:sensor_id`
- Message format: JSON matching the shared schema
- The payload must preserve anomaly markers produced in Phase 2

## Functional Requirements
- Add Kafka producer support to the existing simulator flow.
- Allow local Kafka or managed Kafka configuration through config/env settings.
- Provide a simple validation path to prove messages can be consumed successfully.
- Keep publishing logic small and attached to the existing simulator, not a separate replacement app.
- Preserve continuous streaming behavior for both the normal `15`-second cadence and the `1`-second load-test cadence from Phase 2.

## What This Phase Must Not Do
- Do not start Spark ingestion yet.
- Do not transform, clean, or enrich the payload before Kafka beyond what the simulator already does.
- Do not change the schema fields to suit Kafka.

## Exit Artifacts
- Kafka producer code attached to the simulator
- `shared/config/kafka.yaml`
- topic documentation in `shared/data/kafka_topics.md`
- validation notes in `shared/data/kafka_validation.md`

## Verification
- Publish simulator events into `sensor.telemetry.raw`.
- Consume a sample of those events back out.
- Confirm the consumed payload still matches `shared/schemas/sensor_event.schema.json`.
- Confirm the partition key is stable for the same greenhouse, zone, and sensor.

## Handoff To Phase 4
Phase 4 must read from:
- topic `sensor.telemetry.raw`
- the payload format already published here
- the schema already defined in Phase 1

Phase 4 must not redefine the Kafka payload shape.

## Completion Notes
- Status: Complete
- Entrypoint: `python simulator/src/main.py --publish-kafka`
- Dry-run verification: `python simulator/src/main.py --events 100 --seed 42 --mode load --publish-kafka --kafka-dry-run`
- Existing simulator verification remains: `python simulator/src/main.py --events 2400 --seed 42 --mode normal --check`
- Live validation command on the EC2/MSK client host: `python simulator/src/main.py --events 100 --seed 42 --mode load --publish-kafka`
- Consume validation command on the EC2/MSK client host: `python3.11 simulator/src/main.py --events 10 --consume-kafka`
- Note: the current MSK cluster topic may be `greenhouse.sensor-events.v1`; the canonical phase topic remains `sensor.telemetry.raw` for Phase 4 unless the plan is intentionally changed.
- Live publish evidence: `python3.11 simulator/src/main.py --events 10 --seed 42 --mode load --publish-kafka` published to `greenhouse.sensor-events.v1`, and `kafka-get-offsets.sh` showed offsets across partitions `0`, `1`, and `2`.
- Live consume evidence: `python3.11 simulator/src/main.py --events 10 --consume-kafka` consumed 10 schema-valid events from `greenhouse.sensor-events.v1`.
