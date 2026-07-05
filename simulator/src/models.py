from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = REPO_ROOT / "shared" / "schemas" / "sensor_event.schema.json"
GREENHOUSES_PATH = REPO_ROOT / "shared" / "config" / "greenhouses.yaml"
THRESHOLDS_PATH = REPO_ROOT / "shared" / "config" / "anomaly_thresholds.yaml"


@dataclass(frozen=True)
class Sensor:
    sensor_id: str
    metric_family: str
    greenhouse_id: str
    zone_id: str


@dataclass(frozen=True)
class Zone:
    zone_id: str
    name: str
    sensors: tuple[Sensor, ...]


@dataclass(frozen=True)
class Greenhouse:
    greenhouse_id: str
    name: str
    crop: str
    zones: tuple[Zone, ...]


@dataclass(frozen=True)
class SimulatorConfig:
    greenhouses: tuple[Greenhouse, ...]
    thresholds: dict
    schema: dict

    @property
    def sensors(self) -> tuple[Sensor, ...]:
        return tuple(
            sensor
            for greenhouse in self.greenhouses
            for zone in greenhouse.zones
            for sensor in zone.sensors
        )
