# Kafka Validation

## Local dry run

```powershell
python simulator/src/main.py --events 100 --seed 42 --mode load --publish-kafka --kafka-dry-run
```

This validates the generated payloads against `shared/schemas/sensor_event.schema.json` and computes the same partition key used for Kafka publishing.

## MSK publish

Install the Python client dependencies on the host that can reach MSK:

```bash
pip install "confluent-kafka<2.13" aws-msk-iam-sasl-signer-python
```

On the EC2 MSK client host, the simulator uses the installed Kafka CLI when `/opt/kafka/bin/kafka-console-producer.sh` and `~/client.properties` exist. This matches the Java IAM client path already used to create the topic.

Then publish:

```bash
python simulator/src/main.py --events 100 --seed 42 --mode load --publish-kafka
```

Consume and schema-check a sample:

```bash
python simulator/src/main.py --events 10 --consume-kafka
```

Required MSK env values:

```env
AWS_REGION=ap-south-1
KAFKA_BOOTSTRAP_SERVERS=boot-ikhrabgh.c3.kafka-serverless.ap-south-1.amazonaws.com:9098
KAFKA_TOPIC=greenhouse.sensor-events.v1
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=AWS_MSK_IAM
```

If consume validation fails with `GroupAuthorizationException`, add IAM group permissions for the validation consumer group, including `kafka-cluster:DescribeGroup` and `kafka-cluster:AlterGroup`.
