import argparse
from datetime import datetime, timezone
from pathlib import Path
import time

from anomalies import REQUIRED_ANOMALIES
from generator import MODES, generate_events, load_config, validate_event, write_events
from models import REPO_ROOT


DEFAULT_OUTPUT = REPO_ROOT / "simulator" / "output" / "sample_events.jsonl"
DEFAULT_START = datetime(2026, 1, 1, tzinfo=timezone.utc)


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
    parser.add_argument("--events", type=int, default=300, help="number of events to write")
    parser.add_argument("--seed", type=int, default=20260705, help="deterministic random seed")
    parser.add_argument("--mode", choices=MODES, default="normal", help="normal=15s per sensor, load=1s per sensor")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="newline-delimited JSON output path")
    parser.add_argument("--start-time", default=None, help="ISO timestamp, default is 2026-01-01T00:00:00+00:00")
    parser.add_argument("--realtime", action="store_true", help="sleep between full sensor ticks")
    parser.add_argument("--check", action="store_true", help="run reproducibility, topology, schema, and anomaly checks")
    return parser.parse_args()


def run_self_check(config, *, seed: int, interval_seconds: int, start_time: datetime) -> None:
    first = list(generate_events(config, event_count=300, seed=seed, interval_seconds=interval_seconds, start_time=start_time))
    second = list(generate_events(config, event_count=300, seed=seed, interval_seconds=interval_seconds, start_time=start_time))
    assert first == second, "same seed/config did not reproduce identical output"
    assert len(config.sensors) == 100, f"expected 100 sensors, found {len(config.sensors)}"

    for event in first:
        validate_event(event, config.schema)

    observed = {event["anomaly_type"] for event in first if event["is_injected_anomaly"]}
    missing = REQUIRED_ANOMALIES - observed
    assert not missing, f"missing injected anomaly types: {sorted(missing)}"


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


if __name__ == "__main__":
    raise SystemExit(main())
