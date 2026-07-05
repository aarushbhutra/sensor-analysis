REQUIRED_ANOMALIES = {
    "temperature_high",
    "humidity_out_of_range",
    "soil_moisture_low",
    "co2_out_of_range",
    "light_out_of_range",
    "battery_low",
    "missing_data",
}


SCENARIOS = (
    {"type": "temperature_high", "start": 2, "duration": 9, "zone": "gh-001-a", "families": {"climate", "air", "device_health"}},
    {"type": "humidity_out_of_range", "start": 3, "duration": 10, "zone": "gh-001-b", "families": {"climate", "air", "light"}},
    {"type": "soil_moisture_low", "start": 4, "duration": 13, "zone": "gh-002-c", "families": {"soil", "climate", "device_health"}},
    {"type": "co2_out_of_range", "start": 5, "duration": 9, "zone": "gh-003-b", "families": {"air", "climate"}},
    {"type": "light_out_of_range", "start": 6, "duration": 10, "zone": "gh-004-d", "families": {"light", "climate", "air"}},
    {"type": "missing_data", "start": 8, "duration": 5, "sensor": "gh-003-e-health-01"},
    {"type": "battery_low", "start": 2, "duration": 22, "sensor": "gh-004-e-health-01"},
)


def apply_anomaly(state: dict, sensor, tick: int, thresholds: dict) -> str | None:
    for scenario in SCENARIOS:
        if not _matches(scenario, sensor):
            continue
        start, end = _window(scenario, sensor)
        strength = _strength(start, end, tick, sensor.sensor_id + scenario["type"])
        if strength <= 0:
            continue
        anomaly_type = scenario["type"]
        _apply(state, anomaly_type, strength, thresholds)
        if anomaly_type == "battery_low" and state["battery_level"] >= thresholds["battery_level"]["min"]:
            return None
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
        state["battery_level"] = max(0.0, state["battery_level"] - 0.65 - 0.45 * strength)
    elif anomaly_type == "missing_data":
        state["location_status"] = "offline"


def _matches(window: dict, sensor) -> bool:
    if "sensor" in window:
        return sensor.sensor_id == window["sensor"]
    if "zone" in window:
        if sensor.zone_id != window["zone"]:
            return False
        return sensor.metric_family in window["families"]
    if "family" in window:
        return sensor.metric_family == window["family"]
    return False


def _window(scenario: dict, sensor) -> tuple[int, int]:
    jitter = _stable_number(sensor.sensor_id + scenario["type"]) % 5
    duration = scenario["duration"] + (_stable_number(scenario["type"] + sensor.sensor_id) % 4)
    if scenario["type"] == "battery_low":
        jitter = 0
    start = scenario["start"] + jitter
    return start, start + duration


def _strength(start: int, end: int, tick: int, key: str) -> float:
    if tick < start or tick > end:
        return 0.0
    span = max(1, end - start)
    progress = (tick - start) / span
    ramp = min(1.0, progress * 2.2) if progress < 0.55 else max(0.25, 1.0 - (progress - 0.55) * 1.4)
    wobble = 0.85 + (_stable_number(key) % 7) * 0.04
    return ramp * wobble


def _stable_number(value: str) -> int:
    return sum((index + 1) * ord(char) for index, char in enumerate(value))
