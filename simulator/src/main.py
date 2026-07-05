import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import time

from anomalies import REQUIRED_ANOMALIES
from generator import MODES, generate_events, load_config, validate_event, write_events
from models import REPO_ROOT


DEFAULT_OUTPUT = REPO_ROOT / "simulator" / "output" / "sample_events.jsonl"
DEFAULT_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
CHECK_EVENTS = 2400


def main() -> int:
    args = parse_args()
    config = load_config()
    start_time = datetime.fromisoformat(args.start_time) if args.start_time else DEFAULT_START
    interval_seconds = MODES[args.mode]

    events = _validated_events(
        config,
        event_count=args.events,
        seed=args.seed,
        interval_seconds=interval_seconds,
        start_time=start_time,
        realtime=args.realtime,
    )
    count = write_events(args.output, events)

    if args.check:
        run_self_check(config, seed=args.seed, interval_seconds=interval_seconds, start_time=start_time)

    print(f"wrote {count} events to {args.output}")
    return 0


def parse_args():
    parser = argparse.ArgumentParser(description="Generate deterministic greenhouse sensor telemetry.")
    parser.add_argument("--events", type=int, default=CHECK_EVENTS, help="number of events to write")
    parser.add_argument("--seed", type=int, default=20260705, help="deterministic random seed")
    parser.add_argument("--mode", choices=MODES, default="normal", help="normal=15s per sensor, load=1s per sensor")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="newline-delimited JSON output path")
    parser.add_argument("--start-time", default=None, help="ISO timestamp, default is 2026-01-01T00:00:00+00:00")
    parser.add_argument("--realtime", action="store_true", help="sleep between full sensor ticks")
    parser.add_argument("--check", action="store_true", help="run reproducibility, topology, schema, and anomaly checks")
    return parser.parse_args()


def run_self_check(config, *, seed: int, interval_seconds: int, start_time: datetime) -> None:
    first = list(generate_events(config, event_count=CHECK_EVENTS, seed=seed, interval_seconds=interval_seconds, start_time=start_time))
    second = list(generate_events(config, event_count=CHECK_EVENTS, seed=seed, interval_seconds=interval_seconds, start_time=start_time))
    assert first == second, "same seed/config did not reproduce identical output"
    assert _jsonl_bytes(first) == _jsonl_bytes(second), "same seed/config did not reproduce byte-identical JSONL"
    assert len(config.sensors) == 100, f"expected 100 sensors, found {len(config.sensors)}"

    for event in first:
        validate_event(event, config.schema)

    observed = {event["anomaly_type"] for event in first if event["is_injected_anomaly"]}
    missing = REQUIRED_ANOMALIES - observed
    assert not missing, f"missing injected anomaly types: {sorted(missing)}"
    _assert_anomaly_breaches(first, config.thresholds)
    _assert_staggered_anomalies(first)
    _assert_battery_monotonic(first)
    _assert_persistent_anomalies(first)
    _assert_gradual_recovery(first, "temperature_high", "temperature_c", 4.0)
    _assert_gradual_recovery(first, "humidity_out_of_range", "humidity_pct", 8.0)
    _assert_gradual_recovery(first, "soil_moisture_low", "soil_moisture_pct", 8.0)
    _assert_normal_variability(first)


def _validated_events(config, *, event_count: int, seed: int, interval_seconds: int, start_time: datetime, realtime: bool):
    last_tick = -1
    for index, event in enumerate(
        generate_events(config, event_count=event_count, seed=seed, interval_seconds=interval_seconds, start_time=start_time)
    ):
        validate_event(event, config.schema)
        tick = index // len(config.sensors)
        if realtime and tick != last_tick and last_tick != -1:
            time.sleep(interval_seconds)
        last_tick = tick
        yield event


def _jsonl_bytes(events: list[dict]) -> bytes:
    text = "".join(json.dumps(event, separators=(",", ":"), sort_keys=True) + "\n" for event in events)
    return text.encode("utf-8")


def _by_sensor(events: list[dict]) -> dict[str, list[dict]]:
    grouped = defaultdict(list)
    for event in events:
        grouped[event["sensor_id"]].append(event)
    return grouped


def _assert_battery_monotonic(events: list[dict]) -> None:
    last_by_sensor = {}
    for event in events:
        sensor_id = event["sensor_id"]
        battery = event["battery_level"]
        if sensor_id in last_by_sensor and battery > last_by_sensor[sensor_id] + 0.001:
            raise AssertionError(f"battery jumped upward for {sensor_id}: {last_by_sensor[sensor_id]} -> {battery}")
        last_by_sensor[sensor_id] = battery


def _assert_anomaly_breaches(events: list[dict], thresholds: dict) -> None:
    checks = {
        "temperature_high": lambda event: event["temperature_c"] > thresholds["temperature_c"]["max"],
        "humidity_out_of_range": lambda event: event["humidity_pct"] < thresholds["humidity_pct"]["min"],
        "soil_moisture_low": lambda event: event["soil_moisture_pct"] < thresholds["soil_moisture_pct"]["min"],
        "co2_out_of_range": lambda event: event["co2_ppm"] > thresholds["co2_ppm"]["max"],
        "light_out_of_range": lambda event: event["light_lux"] > thresholds["light_lux"]["max_day"],
        "battery_low": lambda event: event["battery_level"] < thresholds["battery_level"]["min"],
        "missing_data": lambda event: event["location_status"] == "offline",
    }
    for anomaly_type, check in checks.items():
        if not any(event["anomaly_type"] == anomaly_type and check(event) for event in events):
            raise AssertionError(f"{anomaly_type} did not produce an identifiable threshold breach")


def _assert_persistent_anomalies(events: list[dict]) -> None:
    for rows in _by_sensor(events).values():
        streak = 0
        current = None
        for event in rows:
            anomaly_type = event["anomaly_type"]
            if anomaly_type and anomaly_type == current:
                streak += 1
            elif anomaly_type:
                current = anomaly_type
                streak = 1
            else:
                current = None
                streak = 0
            if streak >= 2:
                return
    raise AssertionError("no anomaly persisted across multiple readings for the same sensor")


def _assert_staggered_anomalies(events: list[dict]) -> None:
    starts_by_type = defaultdict(dict)
    for sensor_id, rows in _by_sensor(events).items():
        for tick, event in enumerate(rows):
            anomaly_type = event["anomaly_type"]
            if anomaly_type in {"battery_low", "missing_data", None}:
                continue
            starts_by_type[anomaly_type].setdefault(sensor_id, tick)

    for anomaly_type, starts in starts_by_type.items():
        if len(starts) < 2:
            raise AssertionError(f"{anomaly_type} only affected one sensor")
        if len(set(starts.values())) < 2:
            raise AssertionError(f"{anomaly_type} starts on the same tick for every affected sensor")


def _assert_gradual_recovery(events: list[dict], anomaly_type: str, metric: str, max_jump: float) -> None:
    checked = False
    for rows in _by_sensor(events).values():
        for index, event in enumerate(rows[:-1]):
            next_event = rows[index + 1]
            if event["anomaly_type"] == anomaly_type and next_event["anomaly_type"] != anomaly_type:
                jump = abs(next_event[metric] - event[metric])
                if jump > max_jump:
                    raise AssertionError(f"{metric} reset too fast after {anomaly_type}: {jump}")
                checked = True
    if not checked:
        raise AssertionError(f"no post-anomaly recovery window found for {anomaly_type}")


def _assert_normal_variability(events: list[dict]) -> None:
    for rows in _by_sensor(events).values():
        normal = [event for event in rows if not event["is_injected_anomaly"]]
        if len(normal) < 3:
            continue
        temps = [event["temperature_c"] for event in normal]
        humidity = [event["humidity_pct"] for event in normal]
        if max(temps) - min(temps) > 0.2 and max(humidity) - min(humidity) > 0.2:
            return
    raise AssertionError("normal readings do not show enough deterministic variability")


if __name__ == "__main__":
    raise SystemExit(main())
