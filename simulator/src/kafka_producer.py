import json
import os
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from generator import validate_event
from models import REPO_ROOT


KAFKA_CONFIG_PATH = REPO_ROOT / "shared" / "config" / "kafka.yaml"
DEFAULT_TOPIC = "sensor.telemetry.raw"
DEFAULT_SECURITY_PROTOCOL = "PLAINTEXT"


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    topic: str
    security_protocol: str
    sasl_mechanism: str
    aws_region: str
    client_id: str


@dataclass(frozen=True)
class PublishResult:
    count: int
    topic: str
    first_key: str | None
    last_key: str | None


def load_kafka_settings() -> KafkaSettings:
    _load_dotenv(REPO_ROOT / ".env")
    config = _load_simple_yaml(KAFKA_CONFIG_PATH)
    return KafkaSettings(
        bootstrap_servers=_setting("KAFKA_BOOTSTRAP_SERVERS", config, "bootstrap_servers", ""),
        topic=_setting("KAFKA_TOPIC", config, "topic", DEFAULT_TOPIC),
        security_protocol=_setting("KAFKA_SECURITY_PROTOCOL", config, "security_protocol", DEFAULT_SECURITY_PROTOCOL),
        sasl_mechanism=_setting("KAFKA_SASL_MECHANISM", config, "sasl_mechanism", ""),
        aws_region=_setting("AWS_REGION", config, "aws_region", "ap-south-1"),
        client_id=_setting("KAFKA_CLIENT_ID", config, "client_id", f"sensor-simulator-{socket.gethostname()}"),
    )


def publish_events(events, settings: KafkaSettings, schema: dict, *, dry_run: bool = False) -> PublishResult:
    first_key = None
    last_key = None
    count = 0
    producer = None if dry_run else _make_producer(settings)

    try:
        for event in events:
            validate_event(event, schema)
            key = partition_key(event)
            first_key = first_key or key
            last_key = key
            count += 1
            if producer:
                producer.send(settings.topic, key=key, value=event)
        if producer:
            producer.flush()
    finally:
        if producer:
            producer.close()

    return PublishResult(count=count, topic=settings.topic, first_key=first_key, last_key=last_key)


def consume_events(settings: KafkaSettings, schema: dict, *, limit: int, timeout_seconds: float) -> int:
    consumer = _make_consumer(settings, limit=limit)
    if hasattr(consumer, "read_messages"):
        return _consume_cli(consumer, schema, limit=limit, timeout_seconds=timeout_seconds)
    if hasattr(consumer, "poll_event"):
        return _consume_confluent(consumer, schema, limit=limit, timeout_seconds=timeout_seconds)

    count = 0

    try:
        for message in consumer:
            event = json.loads(message.value.decode("utf-8"))
            validate_event(event, schema)
            count += 1
            if count >= limit:
                return count
    finally:
        consumer.close()

    return count


def partition_key(event: dict) -> str:
    return f"{event['greenhouse_id']}:{event['zone_id']}:{event['sensor_id']}"


def _make_producer(settings: KafkaSettings):
    cli = _make_cli_producer(settings)
    if cli:
        return cli

    confluent = _make_confluent_producer(settings)
    if confluent:
        return confluent

    try:
        from kafka import KafkaProducer
    except ImportError as error:
        raise RuntimeError("Install Kafka client deps: pip install confluent-kafka aws-msk-iam-sasl-signer-python") from error

    return KafkaProducer(
        **_client_config(settings),
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda event: json.dumps(event, separators=(",", ":"), sort_keys=True).encode("utf-8"),
    )


def _make_consumer(settings: KafkaSettings, *, limit: int):
    cli = _make_cli_consumer(settings, limit=limit)
    if cli:
        return cli

    confluent = _make_confluent_consumer(settings)
    if confluent:
        return confluent

    try:
        from kafka import KafkaConsumer
    except ImportError as error:
        raise RuntimeError("Install Kafka client deps: pip install confluent-kafka aws-msk-iam-sasl-signer-python") from error

    return KafkaConsumer(
        settings.topic,
        **_client_config(settings),
        group_id=f"{settings.client_id}-validation",
        auto_offset_reset="earliest",
        consumer_timeout_ms=int(timeout_seconds * 1000),
    )


class _KafkaCliProducer:
    def __init__(self, command: list[str]):
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, text=True, encoding="utf-8")

    def send(self, topic: str, *, key: str, value: dict) -> None:
        if self.process.stdin is None:
            raise RuntimeError("Kafka CLI producer stdin is closed")
        payload = json.dumps(value, separators=(",", ":"), sort_keys=True)
        self.process.stdin.write(f"{key}\t{payload}\n")

    def flush(self) -> None:
        if self.process.stdin:
            self.process.stdin.close()
        try:
            return_code = self.process.wait(timeout=30)
        except subprocess.TimeoutExpired as error:
            self.process.kill()
            raise RuntimeError("Kafka CLI producer timed out") from error
        if return_code:
            raise RuntimeError(f"Kafka CLI producer exited with status {return_code}")

    def close(self) -> None:
        if self.process.poll() is None:
            self.flush()


class _KafkaCliConsumer:
    def __init__(self, command: list[str]):
        self.command = command

    def read_messages(self, timeout_seconds: float) -> list[str]:
        result = subprocess.run(
            self.command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout_seconds + 5,
        )
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        if result.returncode and not lines:
            raise RuntimeError(result.stderr.strip() or f"Kafka CLI consumer exited with status {result.returncode}")
        return lines


class _ConfluentProducer:
    def __init__(self, producer):
        self.producer = producer
        self.errors = []

    def send(self, topic: str, *, key: str, value: dict) -> None:
        payload = json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
        self.producer.produce(topic, key=key.encode("utf-8"), value=payload, callback=self._delivery_report)
        self.producer.poll(0)

    def flush(self) -> None:
        remaining = self.producer.flush(30)
        if self.errors:
            raise RuntimeError(f"Kafka delivery failed: {self.errors[0]}")
        if remaining:
            raise RuntimeError(f"Kafka delivery timed out with {remaining} message(s) still queued")

    def close(self) -> None:
        self.flush()

    def _delivery_report(self, error, message) -> None:
        if error is not None:
            self.errors.append(error)


class _ConfluentConsumer:
    def __init__(self, consumer, topic: str):
        self.consumer = consumer
        self.consumer.subscribe([topic])

    def poll_event(self, timeout_seconds: float):
        return self.consumer.poll(timeout_seconds)

    def close(self) -> None:
        self.consumer.close()


def _make_confluent_producer(settings: KafkaSettings):
    try:
        from confluent_kafka import Producer
    except ImportError:
        return None
    return _ConfluentProducer(Producer(_confluent_config(settings)))


def _make_cli_producer(settings: KafkaSettings):
    command_path = Path(os.environ.get("KAFKA_CONSOLE_PRODUCER", "/opt/kafka/bin/kafka-console-producer.sh"))
    config_path = Path(os.environ.get("KAFKA_CLIENT_PROPERTIES", str(Path.home() / "client.properties")))
    if not command_path.exists() or not config_path.exists():
        return None
    command = [
        str(command_path),
        "--bootstrap-server",
        settings.bootstrap_servers,
        "--producer.config",
        str(config_path),
        "--topic",
        settings.topic,
        "--property",
        "parse.key=true",
        "--property",
        "key.separator=\t",
    ]
    return _KafkaCliProducer(command)


def _make_cli_consumer(settings: KafkaSettings, *, limit: int):
    command_path = Path(os.environ.get("KAFKA_CONSOLE_CONSUMER", "/opt/kafka/bin/kafka-console-consumer.sh"))
    config_path = Path(os.environ.get("KAFKA_CLIENT_PROPERTIES", str(Path.home() / "client.properties")))
    if not command_path.exists() or not config_path.exists():
        return None
    command = [
        str(command_path),
        "--bootstrap-server",
        settings.bootstrap_servers,
        "--consumer.config",
        str(config_path),
        "--topic",
        settings.topic,
        "--from-beginning",
        "--max-messages",
        str(limit),
    ]
    return _KafkaCliConsumer(command)


def _make_confluent_consumer(settings: KafkaSettings):
    try:
        from confluent_kafka import Consumer
    except ImportError:
        return None
    config = _confluent_config(settings)
    config.update(
        {
            "group.id": f"{settings.client_id}-validation",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    return _ConfluentConsumer(Consumer(config), settings.topic)


def _consume_confluent(consumer: _ConfluentConsumer, schema: dict, *, limit: int, timeout_seconds: float) -> int:
    deadline = time.monotonic() + timeout_seconds
    count = 0
    try:
        while time.monotonic() < deadline and count < limit:
            message = consumer.poll_event(1.0)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())
            event = json.loads(message.value().decode("utf-8"))
            validate_event(event, schema)
            count += 1
    finally:
        consumer.close()
    return count


def _consume_cli(consumer: _KafkaCliConsumer, schema: dict, *, limit: int, timeout_seconds: float) -> int:
    messages = consumer.read_messages(timeout_seconds)
    count = 0
    for line in messages:
        event = json.loads(line)
        validate_event(event, schema)
        count += 1
        if count >= limit:
            return count
    return count


def _confluent_config(settings: KafkaSettings) -> dict:
    config = {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": settings.client_id,
        "security.protocol": settings.security_protocol,
        "message.timeout.ms": 30000,
        "request.timeout.ms": 30000,
        "socket.timeout.ms": 30000,
    }
    if settings.security_protocol == "SASL_SSL" and settings.sasl_mechanism.upper() in {"AWS_MSK_IAM", "OAUTHBEARER"}:
        config["sasl.mechanisms"] = "OAUTHBEARER"
        config["oauth_cb"] = _oauth_callback(settings.aws_region)
    elif settings.sasl_mechanism:
        config["sasl.mechanisms"] = settings.sasl_mechanism
    return config


def _oauth_callback(aws_region: str):
    try:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    except ImportError as error:
        raise RuntimeError("Install MSK IAM signer: pip install aws-msk-iam-sasl-signer-python") from error

    def callback(_oauth_config):
        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(aws_region)
        return token, expiry_ms / 1000

    return callback


def _client_config(settings: KafkaSettings) -> dict:
    config = {
        "bootstrap_servers": settings.bootstrap_servers,
        "client_id": settings.client_id,
        "security_protocol": settings.security_protocol,
        "api_version": (3, 6, 0),
    }
    mechanism = settings.sasl_mechanism.upper()
    if settings.security_protocol == "SASL_SSL" and mechanism in {"AWS_MSK_IAM", "OAUTHBEARER"}:
        config["sasl_mechanism"] = "OAUTHBEARER"
        config["sasl_oauth_token_provider"] = _make_token_provider(settings.aws_region)
    elif mechanism:
        config["sasl_mechanism"] = mechanism
    return config


def _make_token_provider(aws_region: str):
    try:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    except ImportError as error:
        raise RuntimeError("Install MSK IAM signer: pip install aws-msk-iam-sasl-signer-python") from error

    try:
        from kafka.sasl.oauth import AbstractTokenProvider
    except ImportError:
        from kafka.oauth.abstract import AbstractTokenProvider

    class MSKTokenProvider(AbstractTokenProvider):
        def token(self) -> str:
            token, _ = MSKAuthTokenProvider.generate_auth_token(aws_region)
            return token

    return MSKTokenProvider()


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _load_simple_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    values = {}
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _setting(env_key: str, config: dict, config_key: str, default: str) -> str:
    return os.environ.get(env_key) or config.get(config_key) or default
