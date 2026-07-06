"""Spark Structured Streaming job from Kafka to the bronze Delta table."""

from __future__ import annotations

import os
import sys
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(globals().get("__file__", ".")).resolve().parents[2]
KAFKA_CONFIG_PATH = REPO_ROOT / "shared" / "config" / "kafka.yaml"
DEFAULT_BUCKET = "sensor-data-lake-dev-859037107576"


@dataclass(frozen=True)
class BronzeIngestSettings:
    bootstrap_servers: str
    topic: str
    security_protocol: str
    sasl_mechanism: str
    output_path: str
    checkpoint_path: str
    table_name: str
    starting_offsets: str
    trigger_interval: str


def load_settings(argv: list[str] | None = None) -> BronzeIngestSettings:
    args = _parse_args(argv)
    config = _load_simple_yaml(KAFKA_CONFIG_PATH)
    bucket = args.sensor_datalake_bucket or os.getenv("SENSOR_DATALAKE_BUCKET", DEFAULT_BUCKET)
    return BronzeIngestSettings(
        bootstrap_servers=args.kafka_bootstrap_servers
        or _setting("KAFKA_BOOTSTRAP_SERVERS", config, "bootstrap_servers"),
        topic=args.kafka_topic or _setting("KAFKA_TOPIC", config, "topic", "greenhouse.sensor-events.v1"),
        security_protocol=args.kafka_security_protocol
        or _setting("KAFKA_SECURITY_PROTOCOL", config, "security_protocol", "PLAINTEXT"),
        sasl_mechanism=args.kafka_sasl_mechanism or _setting("KAFKA_SASL_MECHANISM", config, "sasl_mechanism", ""),
        output_path=args.bronze_output_path
        or os.getenv("BRONZE_OUTPUT_PATH", f"s3://{bucket}/delta/bronze/sensor_events/"),
        checkpoint_path=args.bronze_checkpoint_path
        or os.getenv("BRONZE_CHECKPOINT_PATH", f"s3://{bucket}/checkpoints/bronze_ingest/"),
        table_name=args.bronze_table or os.getenv("BRONZE_TABLE", "bronze.sensor_events"),
        starting_offsets=args.kafka_starting_offsets or os.getenv("KAFKA_STARTING_OFFSETS", "latest"),
        trigger_interval=args.bronze_trigger_interval or os.getenv("BRONZE_TRIGGER_INTERVAL", "30 seconds"),
    )


def build_bronze_frame(spark, settings: BronzeIngestSettings):
    from pyspark.sql.functions import col, current_timestamp, get_json_object

    kafka_options = {
        "kafka.bootstrap.servers": settings.bootstrap_servers,
        "subscribe": settings.topic,
        "startingOffsets": settings.starting_offsets,
        "failOnDataLoss": "false",
    }
    kafka_options.update(_auth_options(settings))

    return (
        spark.readStream.format("kafka")
        .options(**kafka_options)
        .load()
        .select(
            col("value").cast("string").alias("raw_payload"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            current_timestamp().alias("ingest_ts"),
            get_json_object(col("value").cast("string"), "$.event_ts").cast("timestamp").alias("source_event_ts"),
        )
    )


def run(argv: list[str] | None = None) -> None:
    from pyspark.sql import SparkSession

    settings = load_settings(argv)
    if not settings.bootstrap_servers:
        raise ValueError("Set KAFKA_BOOTSTRAP_SERVERS before running bronze ingest.")

    spark = SparkSession.builder.appName("sensor-bronze-stream-ingest").getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_database_name(settings.table_name)}")

    query = (
        build_bronze_frame(spark, settings)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", settings.checkpoint_path)
        .option("path", settings.output_path)
        .trigger(processingTime=settings.trigger_interval)
        .toTable(settings.table_name)
    )
    query.awaitTermination()


def _auth_options(settings: BronzeIngestSettings) -> dict[str, str]:
    protocol = settings.security_protocol.upper()
    mechanism = settings.sasl_mechanism.upper()
    if not protocol or protocol == "PLAINTEXT":
        return {}

    options = {"kafka.security.protocol": protocol}
    if mechanism:
        options["kafka.sasl.mechanism"] = mechanism

    if protocol == "SASL_SSL" and mechanism == "AWS_MSK_IAM":
        options.update(
            {
                "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
                "kafka.sasl.client.callback.handler.class": (
                    "shadedmskiam.software.amazon.msk.auth.iam.IAMClientCallbackHandler"
                ),
            }
        )
    return options


def _database_name(table_name: str) -> str:
    parts = table_name.split(".")
    return ".".join(parts[:-1]) if len(parts) > 1 else "bronze"


def _parse_args(argv: list[str] | None) -> Namespace:
    parser = ArgumentParser(description="Stream greenhouse sensor events from Kafka into bronze Delta.")
    parser.add_argument("--kafka-bootstrap-servers")
    parser.add_argument("--kafka-topic")
    parser.add_argument("--kafka-security-protocol")
    parser.add_argument("--kafka-sasl-mechanism")
    parser.add_argument("--sensor-datalake-bucket")
    parser.add_argument("--bronze-output-path")
    parser.add_argument("--bronze-checkpoint-path")
    parser.add_argument("--bronze-table")
    parser.add_argument("--kafka-starting-offsets")
    parser.add_argument("--bronze-trigger-interval")
    return parser.parse_args(argv)


def _setting(env_name: str, config: dict[str, str], key: str, default: str = "") -> str:
    return os.getenv(env_name, config.get(key, default)).strip()


def _load_simple_yaml(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.split("#", 1)[0].strip()
        if not stripped or ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        values[key.strip()] = value.strip()
    return values


def _demo() -> None:
    settings = BronzeIngestSettings(
        bootstrap_servers="broker:9098",
        topic="greenhouse.sensor-events.v1",
        security_protocol="SASL_SSL",
        sasl_mechanism="AWS_MSK_IAM",
        output_path="s3://bucket/delta/bronze/sensor_events/",
        checkpoint_path="s3://bucket/checkpoints/bronze_ingest/",
        table_name="bronze.sensor_events",
        starting_offsets="latest",
        trigger_interval="30 seconds",
    )
    options = _auth_options(settings)
    assert options["kafka.security.protocol"] == "SASL_SSL"
    assert options["kafka.sasl.mechanism"] == "AWS_MSK_IAM"
    assert _database_name("bronze.sensor_events") == "bronze"
    assert _database_name("sensor_dev.bronze.sensor_events") == "sensor_dev.bronze"
    args = load_settings(["--kafka-bootstrap-servers", "broker:9098", "--bronze-table", "bronze.sensor_events"])
    assert args.bootstrap_servers == "broker:9098"
    assert args.table_name == "bronze.sensor_events"


if __name__ == "__main__":
    if os.getenv("BRONZE_INGEST_SELF_CHECK") == "1":
        _demo()
    else:
        run(sys.argv[1:])
