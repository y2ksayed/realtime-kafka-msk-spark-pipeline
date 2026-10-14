"""Central config for the pipeline.

Everything reads env vars with sane local defaults, so the same code runs
against docker-compose Kafka locally and against MSK on AWS without edits.
"""

import os

# Which environment we're in: "local" or "aws". The streaming job uses this
# to pick checkpoint/output paths and which Kafka packages to load.
ENV = os.getenv("ENV", "local")

# Kafka ----------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "clickstream")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "clickstream-spark-consumer")

# For MSK with IAM auth you'd set this to "iam" and wire in the
# aws-msk-iam-auth jar on the Spark side. Local docker uses plaintext.
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")

# Spark ----------------------------------------------------------------------
APP_NAME = "clickstream-streaming"

# Watermark for late events: 10 minutes is a decent default for clickstream.
# Bump it up if your producers batch or go through spotty networks.
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 minutes")

# Output paths. Locally we just use ./data; on AWS these become s3a:// paths.
S3_BRONZE_PATH = os.getenv("S3_BRONZE_PATH", "./data/bronze")
S3_SILVER_PATH = os.getenv("S3_SILVER_PATH", "./data/silver")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "./data/checkpoints")
QUARANTINE_PATH = os.getenv("QUARANTINE_PATH", "./data/quarantine")

# How often each streaming trigger fires. "30 seconds" keeps local runs snappy;
# in prod I usually go 1-2 minutes to keep S3 file counts sane.
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")

# Spark packages needed for Kafka + S3 + (optionally) Delta.
# Pass these via --packages or spark.jars.packages.
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.1.0"
S3_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"
