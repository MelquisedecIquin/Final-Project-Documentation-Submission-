"""
Spark Structured Streaming job:  Kafka  ->  Spark  ->  MongoDB

Runs inside the `spark` service of docker-compose.yml (nothing to install on your PC).

Two streaming queries share one Kafka source:
  1. RAW         every valid reading is written to  weather.readings
  2. AGGREGATES  1-minute windowed avg/min/max/count per city+metric -> weather.aggregates_1min

Writes use a deterministic _id, so if Spark restarts and re-reads a message the
document is replaced instead of duplicated (idempotent writes).
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather-data")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "weather")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/checkpoints")

SCHEMA = StructType(
    [
        StructField("timestamp", StringType()),
        StructField("value", DoubleType()),
        StructField("metric_type", StringType()),
        StructField("sensor_id", StringType()),
        StructField("location", StringType()),
        StructField("unit", StringType()),
        StructField("observed_at", StringType()),
    ]
)


def write_to_mongo(df, collection):
    (
        df.write.format("mongodb")
        .mode("append")
        .option("connection.uri", MONGO_URI)
        .option("database", MONGO_DB)
        .option("collection", collection)
        .save()
    )


def save_raw(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    write_to_mongo(batch_df, "readings")
    print(f"[raw] batch {batch_id}: wrote {batch_df.count()} readings")


def save_aggregates(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    out = (
        batch_df.withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
        .withColumn(
            "_id",
            F.concat_ws("|", "sensor_id", "metric_type", F.date_format("window_start", "yyyyMMddHHmm")),
        )
    )
    write_to_mongo(out, "aggregates_1min")
    print(f"[agg] batch {batch_id}: upserted {out.count()} windows")


def main():
    spark = (
        SparkSession.builder.appName("WeatherStreaming")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON, drop malformed messages, convert the timestamp string to a real timestamp
    readings = (
        kafka_stream.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("timestamp", F.try_to_timestamp(F.col("timestamp")))  # NULL (not a crash) if malformed
        .dropna(subset=["timestamp", "value", "metric_type", "sensor_id"])
        .withColumn("_id", F.concat_ws("|", "sensor_id", "metric_type", F.date_format("timestamp", "yyyyMMddHHmmss")))
    )

    raw_query = (
        readings.writeStream.foreachBatch(save_raw)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/raw")
        .start()
    )

    aggregates = (
        readings.withWatermark("timestamp", "2 minutes")
        .groupBy(F.window("timestamp", "1 minute"), "sensor_id", "metric_type", "unit")
        .agg(
            F.round(F.avg("value"), 2).alias("avg_value"),
            F.round(F.min("value"), 2).alias("min_value"),
            F.round(F.max("value"), 2).alias("max_value"),
            F.count("*").alias("readings"),
        )
    )

    agg_query = (
        aggregates.writeStream.outputMode("update")
        .foreachBatch(save_aggregates)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/agg")
        .start()
    )

    print(f"Spark streaming started: {KAFKA_BOOTSTRAP}/{KAFKA_TOPIC} -> {MONGO_URI}/{MONGO_DB}")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
