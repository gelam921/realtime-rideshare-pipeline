from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "ride-events"

event_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("fare", DoubleType(), True),
    StructField("event_time", TimestampType(), True),
])


def main():
        spark = SparkSession.builder \
        .appName("RealtimeRidesharePipeline") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), event_schema).alias("data"))
        .select("data.*")
    )

    # Only count completed rides for this aggregation, driver_ping and
    # trip_started don't carry a fare and aren't "a ride" yet.
    completed_rides = parsed_df.filter(col("event_type") == "trip_completed")

    # Watermark bounds how long Spark waits for late data before finalizing
    # a window. Here: 2 minutes past the max event_time seen so far.
    watermarked_df = completed_rides.withWatermark("event_time", "2 minutes")

    windowed_agg = (
        watermarked_df
        .groupBy(
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
        windowed_agg.writeStream
        .format("console")
        .outputMode("update")  # update mode: shows changed windows as late data arrives
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
