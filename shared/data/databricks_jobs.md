# Databricks Jobs

## `stream_ingest_job`

Phase: 4

Task: `bronze_stream_ingest`

Script: `pipeline/bronze/stream_ingest.py`

Purpose: read raw greenhouse telemetry from Kafka and append it to `bronze.sensor_events`.

Trigger: `AvailableNow`, because Databricks serverless jobs do not support infinite `ProcessingTime` streams.

Task parameters:

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

Libraries:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:<match-your-spark-version>
software.amazon.msk:aws-msk-iam-auth:2.3.7
```

Shared/serverless compute requires the shaded MSK IAM callback class, so the script uses:

```text
shadedmskiam.software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

Outputs:

- Table: `bronze.sensor_events`
- Delta path: `s3://sensor-data-lake-dev-859037107576/delta/bronze/sensor_events/`
- Checkpoint path: `s3://sensor-data-lake-dev-859037107576/checkpoints/bronze_ingest/`

Verification:

```sql
SELECT COUNT(*) FROM bronze.sensor_events;
SELECT kafka_topic, kafka_partition, kafka_offset, ingest_ts, source_event_ts
FROM bronze.sensor_events
ORDER BY ingest_ts DESC
LIMIT 10;
```

Restart check:

1. Start the job while the simulator publishes events.
2. Confirm rows appear in `bronze.sensor_events`.
3. Stop the job.
4. Start it again.
5. Confirm offsets continue from the checkpoint instead of replaying from the configured starting offset.
