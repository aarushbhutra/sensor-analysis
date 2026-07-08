# Bronze Pipeline

Phase 4 lands raw greenhouse telemetry from Kafka into the bronze Delta table.

## Job

- Script: `pipeline/bronze/stream_ingest.py`
- Databricks job: `stream_ingest_job`
- Task: `bronze_stream_ingest`
- Source topic: `greenhouse.sensor-events.v1`
- Target table: `bronze.sensor_events`
- Target path: `s3://sensor-data-lake-dev-859037107576/delta/bronze/sensor_events/`
- Checkpoint path: `s3://sensor-data-lake-dev-859037107576/checkpoints/bronze_ingest/`

## Required Parameters

Paste this in the Databricks task **Parameters** field. Replace the bootstrap server value:

```json
[
  "--kafka-bootstrap-servers",
  "<your-msk-bootstrap-brokers>",
  "--kafka-topic",
  "greenhouse.sensor-events.v1",
  "--kafka-security-protocol",
  "SASL_SSL",
  "--kafka-sasl-mechanism",
  "AWS_MSK_IAM",
  "--sensor-datalake-bucket",
  "sensor-data-lake-dev-859037107576",
  "--bronze-table",
  "bronze.sensor_events"
]
```

Optional overrides:

```text
BRONZE_OUTPUT_PATH=s3://sensor-data-lake-dev-859037107576/delta/bronze/sensor_events/
BRONZE_CHECKPOINT_PATH=s3://sensor-data-lake-dev-859037107576/checkpoints/bronze_ingest/
KAFKA_STARTING_OFFSETS=latest
```

## Databricks Libraries

Install these on the job compute when they are not already available:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:<match-your-spark-version>
software.amazon.msk:aws-msk-iam-auth:2.3.7
```

Databricks Shared/serverless compute requires the shaded MSK IAM callback class. The ingest script sets:

```text
kafka.sasl.client.callback.handler.class=shadedmskiam.software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

## Validation

Run the task while the simulator is publishing to Kafka, then query:

```sql
SELECT COUNT(*) FROM bronze.sensor_events;
SELECT raw_payload, kafka_topic, kafka_partition, kafka_offset, ingest_ts, source_event_ts
FROM bronze.sensor_events
ORDER BY ingest_ts DESC
LIMIT 10;
```

Stop and restart the job. The checkpoint at `checkpoints/bronze_ingest/` should let the stream resume without starting from the beginning.

Serverless jobs use `AvailableNow`, so each run drains currently available Kafka records and exits. Run the job again after publishing more simulator data.
