import json
import math
import random
from datetime import datetime, timedelta
from pathlib import Path

from anomalies import apply_anomaly
from models import (
    GREENHOUSES_PATH,
    SCHEMA_PATH,
    THRESHOLDS_PATH,
    Greenhouse,
    Sensor,
    SimulatorConfig,
    Zone,
)


MODES = {"normal": 15, "load": 1}


def load_config() -> SimulatorConfig:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8-sig"))
    thresholds = _parse_thresholds(THRESHOLDS_PATH.read_text(encoding="utf-8-sig"))
    greenhouses = _parse_greenhouses(GREENHOUSES_PATH.read_text(encoding="utf-8-sig"))
    return SimulatorConfig(greenhouses=greenhouses, thresholds=thresholds, schema=schema)


def generate_events(
    config: SimulatorConfig,
    *,
    event_count: int,
    seed: int,
    interval_seconds: int,
    start_time: datetime,
):
    rng = random.Random(seed)
    sensors = config.sensors
    if not sensors:
        raise ValueError("greenhouse topology has no sensors")

    states = {sensor.sensor_id: _initial_state(sensor, rng) for sensor in sensors}

    for event_index in range(event_count):
        sensor = sensors[event_index % len(sensors)]
        tick = event_index // len(sensors)
        event_ts = start_time + timedelta(seconds=tick * interval_seconds)
        state = states[sensor.sensor_id]
        _advance_state(state, sensor, event_ts, rng, config.thresholds)
        anomaly_type = apply_anomaly(state, sensor, tick, config.thresholds)
        yield _event_from_state(sensor, event_index, event_ts, state, anomaly_type, config.thresholds)


def write_events(path: Path, events) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for event in events:
            handle.write(json.dumps(event, separators=(",", ":"), sort_keys=True))
            handle.write("\n")
            count += 1
    return count


def validate_event(event: dict, schema: dict) -> None:
    expected_keys = set(schema["properties"])
    extra = set(event) - expected_keys
    missing = set(schema["required"]) - set(event)
    if extra or missing:
        raise ValueError(f"schema keys mismatch: extra={extra}, missing={missing}")

    for key, rules in schema["properties"].items():
        value = event[key]
        allowed_types = rules["type"] if isinstance(rules["type"], list) else [rules["type"]]
        if not any(_matches_type(value, allowed_type) for allowed_type in allowed_types):
            raise ValueError(f"{key} has invalid type: {value!r}")
        if "enum" in rules and value not in rules["enum"]:
            raise ValueError(f"{key} is outside enum: {value!r}")
        if isinstance(value, (int, float)):
            if "minimum" in rules and value < rules["minimum"]:
                raise ValueError(f"{key} below minimum: {value!r}")
            if "maximum" in rules and value > rules["maximum"]:
                raise ValueError(f"{key} above maximum: {value!r}")
        if key == "event_ts":
            datetime.fromisoformat(value)


def _initial_state(sensor: Sensor, rng: random.Random) -> dict:
    zone_bias = (_stable_number(sensor.zone_id) % 9 - 4) / 10
    sensor_bias = (_stable_number(sensor.sensor_id) % 11 - 5) / 20
    return {
        "zone_bias": zone_bias,
        "sensor_bias": sensor_bias,
        "temperature_c": 22.5 + zone_bias + sensor_bias + rng.uniform(-0.3, 0.3),
        "humidity_pct": 68.0 - zone_bias + rng.uniform(-1.0, 1.0),
        "soil_moisture_pct": (38.0 + rng.uniform(-1.0, 1.0)) if sensor.zone_id == "gh-002-c" else 56.0 + zone_bias + rng.uniform(-1.5, 1.5),
        "co2_ppm": 760.0 + rng.uniform(-30.0, 30.0),
        "light_lux": rng.uniform(0.0, 300.0),
        "battery_level": (23.5 + rng.uniform(0.0, 0.8)) if sensor.sensor_id == "gh-004-e-health-01" else 98.0 + rng.uniform(0.0, 2.0),
        "cloud_cover": rng.uniform(0.05, 0.35),
        "location_status": "online",
    }


def _advance_state(state: dict, sensor: Sensor, event_ts: datetime, rng: random.Random, thresholds: dict) -> None:
    daylight = _daylight(event_ts)
    state["cloud_cover"] = _clamp(state["cloud_cover"] + rng.uniform(-0.04, 0.04), 0.0, 0.8)

    temperature_target = 21.5 + state["zone_bias"] + state["sensor_bias"] + daylight * 5.0
    state["temperature_c"] = _approach(state["temperature_c"], temperature_target, 0.08) + rng.uniform(-0.12, 0.12)

    humidity_target = 70.0 - (state["temperature_c"] - 22.0) * 1.7 - state["zone_bias"]
    state["humidity_pct"] = _clamp(_approach(state["humidity_pct"], humidity_target, 0.07) + rng.uniform(-0.35, 0.35), 0, 100)

    soil_loss = rng.uniform(0.03, 0.11)
    irrigation_gain = 0.5 if state["soil_moisture_pct"] < thresholds["soil_moisture_pct"]["min"] + 5 else 0.0
    state["soil_moisture_pct"] = _clamp(state["soil_moisture_pct"] - soil_loss + irrigation_gain, 0, 100)

    co2_target = 770.0 + state["zone_bias"] * 40.0 + rng.uniform(-55.0, 55.0)
    state["co2_ppm"] = max(0.0, _approach(state["co2_ppm"], co2_target, 0.06) + rng.uniform(-12.0, 12.0))

    if daylight:
        clear_sky = thresholds["light_lux"]["min_day"] + daylight * (thresholds["light_lux"]["max_day"] - thresholds["light_lux"]["min_day"])
        light_target = clear_sky * (1.0 - state["cloud_cover"] * 0.65)
    else:
        light_target = rng.uniform(0.0, thresholds["light_lux"]["max_night"])
    state["light_lux"] = max(0.0, _approach(state["light_lux"], light_target, 0.25) + rng.uniform(-120.0, 120.0))

    drain = 0.012 + (_stable_number(sensor.sensor_id) % 7) * 0.001
    state["battery_level"] = _clamp(state["battery_level"] - drain, 0, 100)
    state["location_status"] = "online"


def _event_from_state(
    sensor: Sensor,
    event_index: int,
    event_ts: datetime,
    state: dict,
    anomaly_type: str | None,
    thresholds: dict,
) -> dict:
    temperature = state["temperature_c"]
    soil = state["soil_moisture_pct"]
    return {
        "event_id": f"{sensor.sensor_id}-{event_ts.strftime('%Y%m%dT%H%M%S')}-{event_index:09d}",
        "event_ts": event_ts.isoformat(),
        "sensor_id": sensor.sensor_id,
        "greenhouse_id": sensor.greenhouse_id,
        "zone_id": sensor.zone_id,
        "metric_family": sensor.metric_family,
        "temperature_c": round(temperature, 2),
        "humidity_pct": round(state["humidity_pct"], 2),
        "soil_moisture_pct": round(soil, 2),
        "co2_ppm": round(state["co2_ppm"], 2),
        "light_lux": round(state["light_lux"], 2),
        "battery_level": round(state["battery_level"], 2),
        "fan_status": "on" if temperature > thresholds["temperature_c"]["max"] else "off",
        "irrigation_status": "on" if soil < thresholds["soil_moisture_pct"]["min"] + 5 else "off",
        "location_status": state["location_status"],
        "is_injected_anomaly": anomaly_type is not None,
        "anomaly_type": anomaly_type,
    }


def _parse_thresholds(text: str) -> dict:
    thresholds: dict[str, dict] = {}
    current = None
    for raw_line in text.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip())
        key, _, raw_value = raw_line.strip().partition(":")
        if indent == 0:
            current = key
            thresholds[current] = {}
        elif indent == 2 and current:
            thresholds[current][key] = _scalar(raw_value.strip())
    return thresholds


def _parse_greenhouses(text: str) -> tuple[Greenhouse, ...]:
    greenhouses = []
    current_greenhouse = None
    current_zone = None
    current_sensor = None

    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#") or stripped == "greenhouses:":
            continue
        indent = len(raw_line) - len(raw_line.lstrip())

        if indent == 2 and stripped.startswith("- greenhouse_id:"):
            current_greenhouse = {"greenhouse_id": _after_colon(stripped), "name": "", "crop": "", "zones": []}
            greenhouses.append(current_greenhouse)
        elif indent == 4 and current_greenhouse and stripped != "zones:":
            key, value = _key_value(stripped)
            current_greenhouse[key] = value
        elif indent == 6 and stripped.startswith("- zone_id:") and current_greenhouse:
            current_zone = {"zone_id": _after_colon(stripped), "name": "", "sensors": []}
            current_greenhouse["zones"].append(current_zone)
        elif indent == 8 and current_zone and stripped != "sensors:":
            key, value = _key_value(stripped)
            current_zone[key] = value
        elif indent == 10 and stripped.startswith("- sensor_id:") and current_zone and current_greenhouse:
            current_sensor = {
                "sensor_id": _after_colon(stripped),
                "metric_family": "",
                "greenhouse_id": current_greenhouse["greenhouse_id"],
                "zone_id": current_zone["zone_id"],
            }
            current_zone["sensors"].append(current_sensor)
        elif indent == 12 and current_sensor:
            key, value = _key_value(stripped)
            current_sensor[key] = value

    return tuple(
        Greenhouse(
            greenhouse_id=greenhouse["greenhouse_id"],
            name=greenhouse["name"],
            crop=greenhouse["crop"],
            zones=tuple(
                Zone(
                    zone_id=zone["zone_id"],
                    name=zone["name"],
                    sensors=tuple(Sensor(**sensor) for sensor in zone["sensors"]),
                )
                for zone in greenhouse["zones"]
            ),
        )
        for greenhouse in greenhouses
    )


def _matches_type(value, allowed_type: str) -> bool:
    if allowed_type == "string":
        return isinstance(value, str)
    if allowed_type == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if allowed_type == "boolean":
        return isinstance(value, bool)
    if allowed_type == "null":
        return value is None
    if allowed_type == "object":
        return isinstance(value, dict)
    return False


def _daylight(event_ts: datetime) -> float:
    hour = event_ts.hour + event_ts.minute / 60 + event_ts.second / 3600
    if hour < 6 or hour > 20:
        return 0.0
    return math.sin(math.pi * (hour - 6) / 14)


def _approach(current: float, target: float, rate: float) -> float:
    return current + (target - current) * rate


def _clamp(value: float, low: float, high: float) -> float:
    return min(high, max(low, value))


def _stable_number(value: str) -> int:
    return sum(ord(char) for char in value)


def _key_value(line: str) -> tuple[str, str]:
    key, _, value = line.partition(":")
    return key, value.strip()


def _after_colon(line: str) -> str:
    return _key_value(line)[1]


def _scalar(value: str):
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value
