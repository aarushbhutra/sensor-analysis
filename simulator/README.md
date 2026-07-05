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
