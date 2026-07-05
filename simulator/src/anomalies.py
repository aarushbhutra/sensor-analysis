REQUIRED_ANOMALIES = {
    "temperature_high",
    "humidity_out_of_range",
    "soil_moisture_low",
    "co2_out_of_range",
    "light_out_of_range",
    "battery_low",
    "missing_data",
}


WINDOWS = (
    {"type": "temperature_high", "start": 1, "peak": 4, "end": 8, "zone": "gh-001-a"},
    {"type": "humidity_out_of_range", "start": 2, "peak": 5, "end": 9, "zone": "gh-001-b"},
    {"type": "soil_moisture_low", "start": 2, "peak": 6, "end": 10, "zone": "gh-002-c"},
    {"type": "co2_out_of_range", "start": 3, "peak": 6, "end": 9, "zone": "gh-003-b"},
    {"type": "light_out_of_range", "start": 3, "peak": 5, "end": 8, "zone": "gh-004-d"},
    {"type": "missing_data", "start": 4, "peak": 5, "end": 7, "sensor": "gh-003-e-health-01"},
    {"type": "battery_low", "start": 1, "peak": 7, "end": 11, "sensor": "gh-004-e-health-01"},
)


def apply_anomaly(state: dict, sensor, tick: int, thresholds: dict) -> str | None:
    for window in WINDOWS:
        if not _matches(window, sensor):
            continue
        strength = _strength(window, tick)
        if strength <= 0:
            continue
        anomaly_type = window["type"]
        _apply(state, anomaly_type, strength, thresholds)
        return anomaly_type
    return None


def _apply(state: dict, anomaly_type: str, strength: float, thresholds: dict) -> None:
    if anomaly_type == "temperature_high":
        state["temperature_c"] += 1.2 + 0.8 * strength
        state["humidity_pct"] = max(0.0, state["humidity_pct"] - 0.6 * strength)
    elif anomaly_type == "humidity_out_of_range":
        state["humidity_pct"] = max(0.0, state["humidity_pct"] - 2.0 - 1.5 * strength)
    elif anomaly_type == "soil_moisture_low":
        state["soil_moisture_pct"] = max(0.0, state["soil_moisture_pct"] - 1.4 - 1.2 * strength)
    elif anomaly_type == "co2_out_of_range":
        state["co2_ppm"] += 80.0 + 70.0 * strength
    elif anomaly_type == "light_out_of_range":
        state["light_lux"] += 18000.0 + 8000.0 * strength
    elif anomaly_type == "battery_low":
        state["battery_level"] = max(0.0, state["battery_level"] - 0.35 - 0.75 * strength)
    elif anomaly_type == "missing_data":
        state["location_status"] = "offline"


def _matches(window: dict, sensor) -> bool:
    if "sensor" in window:
        return sensor.sensor_id == window["sensor"]
    if "zone" in window:
        return sensor.zone_id == window["zone"]
    if "family" in window:
        return sensor.metric_family == window["family"]
    return False


def _strength(window: dict, tick: int) -> float:
    if tick < window["start"] or tick > window["end"]:
        return 0.0
    if tick <= window["peak"]:
        return (tick - window["start"] + 1) / (window["peak"] - window["start"] + 1)
    return (window["end"] - tick + 1) / (window["end"] - window["peak"] + 1)
