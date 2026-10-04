from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, avg, to_date, hour

from validation import validate_events

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "ride-events"

# Local filesystem standing in for MinIO/S3 due to corporate network
# restrictions blocking object storage registries. Same partition scheme
# (date/hour) as a real data lake, swap this path for an s3a:// URI later.
LAKE_PATH = "/Users/gregoryalanelamii/rides-lake/silver/ride_window_agg"
CHECKPOINT_PATH = "/Users/gregoryalanelamii/rides-lake/checkpoints/ride_window_agg"


def write_batch_to_parquet(batch_df, batch_id):
    """
    foreachBatch gives us a plain, static DataFrame per micro-batch,
    which we can control exactly like a batch job: add partition columns,
    choose write mode, etc. This is the hook point for custom sink logic.
    """
    if batch_df.isEmpty():
        print(f"Batch {batch_id} is empty, skipping write.")
        return

    enriched = batch_df.withColumn(
        "window_date", to_date(col("window_start"))
    ).withColumn("window_hour", hour(col("window_start")))

    (
        enriched.write.mode("append")
        .partitionBy("window_date", "window_hour")
        .parquet(LAKE_PATH)
    )

    print(f"Batch {batch_id}: wrote {enriched.count()} rows to {LAKE_PATH}")


def main():
    spark = SparkSession.builder \
        .appName("RealtimeRidesharePipeline") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Bad events are dropped here; raw_stream.py is what records them in
    # the dead-letter queue.
    parsed_df, _bad_df = validate_events(raw_df)

    completed_rides = parsed_df.filter(col("event_type") == "trip_completed")
    watermarked_df = completed_rides.withWatermark("event_time", "2 minutes")

    windowed_agg = (
        watermarked_df.groupBy(
            window(col("event_time"), "5 minutes"),
            col("zone"),
        )
        .agg(
            count("*").alias("rides_completed"),
            avg("fare").alias("avg_fare"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("zone"),
            col("rides_completed"),
            col("avg_fare"),
        )
    )

    query = (
        windowed_agg.writeStream.outputMode("update")
        .foreachBatch(write_batch_to_parquet)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
