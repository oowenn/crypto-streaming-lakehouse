#!/usr/bin/env python3
"""
Silver → Gold: 1-minute OHLCV + VWAP (event-time windows).
"""
import argparse
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, to_date, sum as s, count as c, min as fmin, max as fmax, struct
)

def args_():
    p = argparse.ArgumentParser()
    p.add_argument("--silver", default="data/silver/trades")
    p.add_argument("--gold", default="data/gold/bars_1m")
    p.add_argument("--checkpoint", default="data/checkpoints/silver_to_gold_bars_1m")
    p.add_argument("--trigger", default="5 seconds")
    p.add_argument("--watermark", default="2 minutes")      # accept a bit of lateness
    p.add_argument("--bar", default="1 minute")             # change to 5 minutes, etc.
    return p.parse_args()

def main():
    a = args_()
    spark = (SparkSession.builder.appName("silver→gold(bars_1m)").master("local[*]").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # Stream Silver
    silver_schema = spark.read.parquet(a.silver).schema
    ss = (spark.readStream
      .schema(silver_schema)
      .parquet(a.silver))
    
    # Window by event_time; compute OHLCV, notional, count
    grouped = (
        ss.withWatermark("event_time", a.watermark)
          .groupBy(
              col("symbol"),
              window(col("event_time"), a.bar).alias("w")
          )
          .agg(
              # OHLC via struct-min/max trick (orders by event_time)
              fmin(struct(col("event_time"), col("price"))).alias("open_s"),
              fmax(struct(col("event_time"), col("price"))).alias("close_s"),
              fmin(col("price")).alias("low"),
              fmax(col("price")).alias("high"),
              s(col("size")).alias("volume"),
              s(col("price") * col("size")).alias("notional"),
              c("*").alias("trades")
          )
    )

    bars = (
        grouped.select(
            col("symbol"),
            col("w.start").alias("bar_start"),
            col("w.end").alias("bar_end"),
            col("open_s.price").alias("open"),
            col("high"),
            col("low"),
            col("close_s.price").alias("close"),
            col("volume"),
            (col("notional")/col("volume")).alias("vwap"),
            col("trades"),
        )
        .withColumn("bar_date", to_date(col("bar_start")))
    )

    q = (bars.writeStream
         .format("parquet")
         .option("path", a.gold)
         .option("checkpointLocation", a.checkpoint)
         .partitionBy("bar_date", "symbol")   # fast lookups by day+symbol
         .outputMode("append")
         .trigger(processingTime=a.trigger)
         .start())

    print(f"→ Gold 1m bars at {a.gold} (checkpoint={a.checkpoint}) … Ctrl-C to stop")
    q.awaitTermination()

if __name__ == "__main__":
    main()
