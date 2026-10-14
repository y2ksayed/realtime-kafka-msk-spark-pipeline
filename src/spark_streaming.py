"""Spark Structured Streaming job: Kafka -> bronze -> silver.

What it does:
  1. Reads JSON clickstream events from Kafka.
  2. Writes raw payloads to bronze (parquet, partitioned by date).
  3. Dedups on event_id, enriches, writes clean rows to silver.
  4. Routes malformed events to a quarantine path instead of crashing.

Run locally:
    python src/spark_streaming.py --env local

Run against MSK/S3:
    ENV=aws KAFKA_BOOTSTRAP_SERVERS=<msk-string> python src/spark_streaming.py --env aws

Spark packages (pass via --packages or spark.jars.packages):
    org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4
    (+ io.delta:delta-spark_2.12:3.1.0 if you want Delta for silver)
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

import config

# Fixed schema for the clickstream JSON. Keeping it explicit means a producer
# change shows up as nulls / quarantine rows instead of silent corruption.
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("page", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("user_agent", StringType(), True),
        StructField("country", StringType(), True),
    ]
)


def build_spark(app_name: str) -> SparkSession:
    """Create the Spark session with the bits streaming jobs always need."""
    builder = (
        SparkSession.builder.appName(app_name)
        # Shuffle partitions: 200 is the default and way too high for a small stream.
        .config("spark.sql.shuffle.partitions", "8")
        # Prometheus sink so the Grafana dashboard has something to scrape.
        .config("spark.metrics.conf.*.sink.prometheus.class",
                "org.apache.spark.metrics.sink.PrometheusServlet")
        .config("spark.metrics.conf.*.sink.prometheus.path", "/metrics/prometheus")
    )
    if config.ENV == "aws":
        # S3A settings for writing to S3. IAM role auth is the norm on EMR/EKS.
        builder = builder.config("spark.hadoop.fs.s3a.impl",
                                 "org.apache.hadoop.fs.s3a.S3AFileSystem")
    return builder.getOrCreate()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="local", choices=["local", "aws"])
    args = parser.parse_args()
    config.ENV = args.env

    spark = build_spark(config.APP_NAME)
    spark.sparkContext.setLogLevel("WARN")

    # --- Read from Kafka -----------------------------------------------------
    # startingOffsets=latest is right for a demo; use "earliest" for backfills.
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Kafka value is bytes -> cast to string, then parse JSON against the schema.
    parsed = raw.select(
        F.col("value").cast("string").alias("json_str"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).select(
        F.from_json(F.col("json_str"), EVENT_SCHEMA).alias("e"),
        F.col("json_str"),
        F.col("kafka_timestamp"),
    )

    # Split good rows from malformed ones early, so one bad producer can't
    # kill the whole stream. Bad rows go to quarantine for inspection.
    good = parsed.filter(F.col("e.event_id").isNotNull()).select("e.*", "kafka_timestamp")
    bad = parsed.filter(F.col("e.event_id").isNull()).select("json_str", "kafka_timestamp")

    # Normalize the event timestamp once, here, so bronze/silver agree.
    good = good.withColumn("event_time", F.to_timestamp("event_time"))

    # --- Bronze: raw payloads -------------------------------------------------
    # Keep everything, partitioned by date. This is the replay source if the
    # silver logic ever changes.
    bronze_query = (
        good.withColumn("dt", F.to_date("event_time"))
        .writeStream.format("parquet")
        .option("path", config.S3_BRONZE_PATH)
        .option("checkpointLocation", f"{config.CHECKPOINT_PATH}/bronze")
        .partitionBy("dt")
        .trigger(processingTime=config.TRIGGER_INTERVAL)
        .outputMode("append")
        .start()
    )

    # --- Silver: deduped + enriched ------------------------------------------
    # Watermark handles late arrivals; dropDuplicates on event_id gives us
    # idempotent replays, which matters when Kafka redelivers.
    silver = (
        good.withWatermark("event_time", config.WATERMARK_DELAY)
        .dropDuplicates(["event_id"])
        # Small enrichment: line total and a coarse device bucket.
        .withColumn("line_total", F.col("price") * F.col("quantity"))
        .withColumn(
            "is_purchase",
            F.col("event_type").isin("add_to_cart", "checkout"),
        )
        .withColumn("dt", F.to_date("event_time"))
    )

    silver_query = (
        silver.writeStream.format("parquet")
        .option("path", config.S3_SILVER_PATH)
        .option("checkpointLocation", f"{config.CHECKPOINT_PATH}/silver")
        .partitionBy("dt")
        .trigger(processingTime=config.TRIGGER_INTERVAL)
        .outputMode("append")
        .start()
    )

    # --- Quarantine: malformed events ------------------------------------------
    quarantine_query = (
        bad.writeStream.format("parquet")
        .option("path", config.QUARANTINE_PATH)
        .option("checkpointLocation", f"{config.CHECKPOINT_PATH}/quarantine")
        .trigger(processingTime=config.TRIGGER_INTERVAL)
        .outputMode("append")
        .start()
    )

    print(f"Streaming started (env={config.ENV}). Stop with Ctrl+C.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
