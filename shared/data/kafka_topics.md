# Kafka Topics

## sensor.telemetry.raw

- Purpose: raw greenhouse sensor telemetry from the simulator
- Producer: `python simulator/src/main.py --publish-kafka`
- Partition key: `greenhouse_id:zone_id:sensor_id`
- Message format: JSON matching `shared/schemas/sensor_event.schema.json`
- Anomaly markers: `is_injected_anomaly` and `anomaly_type` are preserved unchanged

For the current MSK Serverless cluster, `KAFKA_TOPIC` may be set to `greenhouse.sensor-events.v1` until the canonical `sensor.telemetry.raw` topic is created.
