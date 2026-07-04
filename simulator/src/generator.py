import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from anomalies import inject_anomaly
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
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    thresholds = _parse_thresholds(THRESHOLDS_PATH.read_text(encoding="utf-8"))
    greenhouses = _parse_greenhouses(GREENHOUSES_PATH.read_text(encoding="utf-8"))
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

    for event_index in range(event_count):
        sensor = sensors[event_index % len(sensors)]
        tick = event_index // len(sensors)
        event_ts = start_time + timedelta(seconds=tick * interval_seconds)
        event = _base_event(sensor, event_index, event_ts, rng, config.thresholds)
        yield inject_anomaly(event, event_index, config.thresholds)


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


def _base_event(sensor: Sensor, event_index: int, event_ts: datetime, rng: random.Random, thresholds: dict) -> dict:
    zone_bias = (_stable_number(sensor.zone_id) % 9 - 4) / 10
    hour = event_ts.hour
    is_day = 6 <= hour < 20

    temperature = 23.0 + zone_bias + rng.uniform(-1.2, 1.2)
    humidity = 67.0 - zone_bias + rng.uniform(-3.0, 3.0)
    soil = 52.0 + zone_bias + rng.uniform(-4.0, 4.0)
    co2 = 760.0 + rng.uniform(-80.0, 80.0)
    light = (28000.0 + rng.uniform(-5000.0, 5000.0)) if is_day else rng.uniform(0.0, 350.0)
    battery = max(20.0, 100.0 - (event_index // 1000) * 0.2 - rng.uniform(0.0, 2.0))

    event = {
        "event_id": f"{sensor.sensor_id}-{event_ts.strftime('%Y%m%dT%H%M%S')}-{event_index:09d}",
        "event_ts": event_ts.isoformat(),
        "sensor_id": sensor.sensor_id,
        "greenhouse_id": sensor.greenhouse_id,
        "zone_id": sensor.zone_id,
        "metric_family": sensor.metric_family,
        "temperature_c": round(temperature, 2),
        "humidity_pct": round(humidity, 2),
        "soil_moisture_pct": round(soil, 2),
        "co2_ppm": round(co2, 2),
        "light_lux": round(light, 2),
        "battery_level": round(battery, 2),
        "fan_status": "on" if temperature > thresholds["temperature_c"]["max"] else "off",
        "irrigation_status": "on" if soil < thresholds["soil_moisture_pct"]["min"] else "off",
        "location_status": "online",
        "is_injected_anomaly": False,
        "anomaly_type": None,
    }
    return event


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
