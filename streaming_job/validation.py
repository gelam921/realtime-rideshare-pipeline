"""
Shared event validation for every Spark job that reads ride-events.

Single source of truth for what counts as a "bad" event. Both
raw_stream.py (which writes the dead-letter queue) and
windowed_aggregation_to_parquet.py (which filters bad events out of the
aggregates) import from here, so the rules can never drift apart.
"""

from pyspark.sql.functions import col, from_json, when, current_timestamp, expr
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

EVENT_SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("fare", DoubleType(), True),
        StructField("event_time", TimestampType(), True),
    ]
)


def validate_events(raw_df):
    """
    Takes the raw Kafka DataFrame and returns (good_df, bad_df).

    good_df: kafka_key + the parsed event columns, only valid events.
    bad_df:  the original payload, one error_reason, Kafka position, and
             failed_at. If an event has several problems, only the first
             matching reason is recorded (order of the when-chain below).
    """
    parsed = raw_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("json_value"),
        col("partition"),
        col("offset"),
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data"),
    )

    validated = parsed.withColumn(
        "error_reason",
        when(expr("try_parse_json(json_value) IS NULL"), "unparseable_json")
        .when(col("data.trip_id").isNull(), "missing_trip_id")
        .when(col("data.zone").isNull(), "missing_zone")
        .when(col("data.event_type").isNull(), "missing_event_type")
        .when(col("data.event_time").isNull(), "missing_or_bad_event_time")
        .when(col("data.fare") < 0, "negative_fare"),
    )

    good_df = validated.filter(col("error_reason").isNull()).select(
        "kafka_key", "data.*"
    )

    bad_df = (
        validated.filter(col("error_reason").isNotNull())
        .select("kafka_key", "json_value", "error_reason", "partition", "offset")
        .withColumn("failed_at", current_timestamp())
    )

    return good_df, bad_df