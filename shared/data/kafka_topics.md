# Kafka Topics

## greenhouse.sensor-events.v1

- Purpose: raw greenhouse sensor telemetry from the simulator
- Producer: `python simulator/src/main.py --publish-kafka`
- Partition key: `greenhouse_id:zone_id:sensor_id`
- Message format: JSON matching `shared/schemas/sensor_event.schema.json`
- Anomaly markers: `is_injected_anomaly` and `anomaly_type` are preserved unchanged

This is the canonical project topic because it is the MSK topic created and validated during Phase 3.
