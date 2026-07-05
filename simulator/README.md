# Greenhouse Sensor Simulator

Generates deterministic newline-delimited JSON telemetry for the 100-sensor greenhouse topology in `shared/config/greenhouses.yaml`.

Run from the repo root:

```powershell
python simulator/src/main.py --events 2400 --seed 42 --mode normal --check
```

Modes:
- `normal`: one event every 15 seconds per sensor
- `load`: one event every 1 second per sensor

The simulator always reads:
- `shared/schemas/sensor_event.schema.json`
- `shared/config/greenhouses.yaml`
- `shared/config/anomaly_thresholds.yaml`

It writes `simulator/output/sample_events.jsonl` by default. Events are full sensor snapshots with explicit `is_injected_anomaly` and `anomaly_type` markers for deterministic demo cases.

Telemetry is stateful: sensor values drift over time, anomaly windows persist across staggered sensor-specific readings, battery drains monotonically before crossing the low threshold, and temperature, humidity, soil moisture, CO2, and light recover gradually instead of snapping back on the next event.

## Kafka Publishing

The same entrypoint can publish generated events to Kafka:

```powershell
python simulator/src/main.py --events 100 --seed 42 --mode load --publish-kafka
```

Kafka settings are read from `.env` first, then `shared/config/kafka.yaml`. For MSK IAM from Python, keep `KAFKA_SECURITY_PROTOCOL=SASL_SSL` and `KAFKA_SASL_MECHANISM=AWS_MSK_IAM`; the simulator maps that to the Python-supported `OAUTHBEARER` flow.

Install the Kafka client packages on the machine that can reach MSK:

```bash
pip install "confluent-kafka<2.13" aws-msk-iam-sasl-signer-python
```

Dry-run validation does not connect to Kafka:

```powershell
python simulator/src/main.py --events 100 --seed 42 --mode load --publish-kafka --kafka-dry-run
```

Consume and schema-check a Kafka sample:

```powershell
python simulator/src/main.py --events 10 --consume-kafka
```
