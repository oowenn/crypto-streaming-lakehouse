#!/usr/bin/env python3
"""
Stream Kafka → Bronze Parquet (raw audit) with Kafka metadata.

Run:
  python spark/jobs/kafka_to_bronze_trades.py \
    --bootstrap localhost:19092 \
    --topic crypto.trades \
    --out data/bronze/crypto_trades \
    --checkpoint data/checkpoints/kafka_to_bronze_trades
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:19092")
    p.add_argument("--topic", default="crypto.trades")
    p.add_argument("--out", default="data/bronze/crypto_trades")
    p.add_argument("--checkpoint", default="data/checkpoints/kafka_to_bronze_trades")
    p.add_argument("--trigger", default="5 seconds")     # micro-batch cadence
    p.add_argument("--startingOffsets", default="latest")# use "earliest" to backfill retained data
    p.add_argument("--maxOffsetsPerTrigger", default=None) # throttle if needed, e.g. "5000"
    return p.parse_args()

def main():
    args = parse_args()

    SPARK_VER = "3.5.1"
    builder = (
        SparkSession.builder
        .appName("kafka→bronze(trades)")
        .master("local[*]")
        # Pull the Kafka source at runtime
        .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VER}")
        # Faster local file commits
        .config("spark.sql.streaming.commitProtocolClass",
                "org.apache.spark.sql.execution.streaming.ManifestFileCommitProtocol")
        .config("spark.sql.streaming.minBatchesToRetain", "2")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.startingOffsets)
        .option("failOnDataLoss", "false")  # friendlier during dev/retention churn
    )
    if args.maxOffsetsPerTrigger:
        reader = reader.option("maxOffsetsPerTrigger", args.maxOffsetsPerTrigger)

    kafka_df = reader.load()

    # Bronze = keep raw bytes (as string) + Kafka metadata
    bronze_df = kafka_df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("ts_kafka"),
        col("timestampType").alias("ts_type"),
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("value_raw"),
        # Uncomment if you later add headers from producers:
        # col("headers")
    )

    query = (
        bronze_df.writeStream
        .format("parquet")
        .option("path", args.out)
        .option("checkpointLocation", args.checkpoint)
        .outputMode("append")
        .trigger(processingTime=args.trigger)
        .start()
    )

    print(f"→ Bronze streaming to {args.out} (checkpoint={args.checkpoint}) … Ctrl-C to stop")
    query.awaitTermination()

if __name__ == "__main__":
    main()
