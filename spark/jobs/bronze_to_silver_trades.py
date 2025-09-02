#!/usr/bin/env python3
"""
Bronze → Silver for trades (parse + types + event-time + dedup).

Run:
  python spark/jobs/bronze_to_silver_trades.py \
    --bronze data/bronze/crypto_trades \
    --silver data/silver/trades \
    --checkpoint data/checkpoints/bronze_to_silver_trades
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType, TimestampType
)
from pyspark.sql.functions import col, from_json, to_timestamp, to_date

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bronze", default="data/bronze/crypto_trades")
    p.add_argument("--silver", default="data/silver/trades")
    p.add_argument("--checkpoint", default="data/checkpoints/bronze_to_silver_trades")
    p.add_argument("--trigger", default="5 seconds")
    p.add_argument("--watermark", default="2 minutes")  # allow a little lateness
    p.add_argument("--no_backfill", action="store_true", help="skip one-time batch backfill")
    p.add_argument("--maxFilesPerTrigger", default=None, help="e.g. '50' to throttle file source")
    return p.parse_args()

# Bronze file schema we created in Step 4
bronze_schema = StructType([
    StructField("topic",      StringType(),    True),
    StructField("partition",  IntegerType(),   True),
    StructField("offset",     LongType(),      True),
    StructField("ts_kafka",   TimestampType(), True),
    StructField("ts_type",    IntegerType(),   True),
    StructField("key",        StringType(),    True),
    StructField("value_raw",  StringType(),    True),
])

# JSON payload shape inside value_raw (from Kraken producer)
payload_schema = StructType([
    StructField("exchange",   StringType(), True),
    StructField("symbol",     StringType(), True),   # e.g. XBT/USDT
    StructField("price",      DoubleType(), True),
    StructField("size",       DoubleType(), True),
    StructField("side",       StringType(), True),
    StructField("order_type", StringType(), True),
    StructField("ts_event",   LongType(),   True),   # ms since epoch
    StructField("ts_ingest",  LongType(),   True),   # ms since epoch
])

def project_to_silver(df):
    parsed = from_json(col("value_raw"), payload_schema).alias("p")
    return (
        df.select("topic","partition","offset","ts_kafka","key","value_raw", parsed)
          .select(
              "topic","partition","offset","ts_kafka","key","value_raw",
              col("p.exchange").alias("exchange"),
              col("p.symbol").alias("symbol"),
              col("p.price").alias("price"),
              col("p.size").alias("size"),
              col("p.side").alias("side"),
              col("p.order_type").alias("order_type"),
              # convert ms → TIMESTAMP
              to_timestamp((col("p.ts_event")/1000).cast("double")).alias("event_time"),
              to_timestamp((col("p.ts_ingest")/1000).cast("double")).alias("ingest_time"),
          )
          .withColumn("event_date", to_date(col("event_time")))
    )

def main():
    args = parse_args()
    spark = (SparkSession.builder
             .appName("bronze→silver(trades)")
             .master("local[*]")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # --------- one-time batch backfill of existing Bronze files ---------
    if not args.no_backfill:
        bronze_batch = spark.read.schema(bronze_schema).parquet(args.bronze)
        silver_batch = project_to_silver(bronze_batch).dropDuplicates(
            ["symbol","event_time","price","size","side"]
        )
        (silver_batch.write.mode("append")
            .partitionBy("event_date")
            .parquet(args.silver))
        print("✓ Backfill complete.")

    # --------- stream new Bronze files → Silver continuously ---------
    reader = (spark.readStream
              .schema(bronze_schema))
    if args.maxFilesPerTrigger:
        bronze_stream = (reader
                         .option("maxFilesPerTrigger", args.maxFilesPerTrigger)
                         .parquet(args.bronze))
    else:
        bronze_stream = reader.parquet(args.bronze)

    silver_stream = (project_to_silver(bronze_stream)
                     .withWatermark("event_time", args.watermark)
                     .dropDuplicates(["symbol","event_time","price","size","side"]))

    q = (silver_stream.writeStream
         .format("parquet")
         .option("path", args.silver)
         .option("checkpointLocation", args.checkpoint)
         .option("compression", "snappy")
         .outputMode("append")
         .partitionBy("event_date")
         .trigger(processingTime=args.trigger)
         .start())

    print(f"→ Silver streaming to {args.silver} (checkpoint={args.checkpoint}) … Ctrl-C to stop")
    q.awaitTermination()

if __name__ == "__main__":
    main()
