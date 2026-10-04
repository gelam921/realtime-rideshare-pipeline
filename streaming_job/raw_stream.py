import os

from pyspark.sql import SparkSession
from validation import validate_events

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "ride-events"
LAKE_ROOT = os.path.expanduser(os.getenv("LAKE_ROOT", "~/rides-lake"))


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

    good_df, bad_df = validate_events(raw_df)

    good_query = (
        good_df.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )

    # This job is the single owner of the dead-letter queue. The
    # aggregation job only filters bad events out; it does not write here,
    # so the DLQ never gets duplicate rows.

    dlq_query = (
        bad_df.writeStream.format("parquet")
        .option("path", f"{LAKE_ROOT}/dlq")
        .option("checkpointLocation", f"{LAKE_ROOT}/_checkpoints/dlq")
        .outputMode("append")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
