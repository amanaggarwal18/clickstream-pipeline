"""
Phase 5 - Silver to Gold: Sessionize events and compute analytics.

Reads the cleaned Parquet events (from clean_events.py), derives real
user sessions from timestamps (30-minute inactivity gap = new session),
and computes two aggregated tables:

  1. sessions       - one row per session, with duration, event count,
                       pages viewed, and whether it converted to a purchase
  2. daily_metrics  - one row per day, with DAU, session counts, and
                       funnel counts/conversion rates across the four
                       event types

Usage (inside the spark-master container):
    spark-submit /opt/spark/jobs/sessionize_events.py \
        --input-path /opt/spark/data/processed \
        --output-path /opt/spark/data/aggregated
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SESSION_GAP_MINUTES = 30


def build_spark_session(app_name: str = "sessionize_events") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def configure_writes(spark: SparkSession):
    # Same two settings from clean_events.py, needed for the same
    # reasons on Windows/Docker bind mounts: avoid whole-directory
    # deletes and whole-directory renames when writing partitioned
    # output. Baking these in from the start this time.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")


def read_cleaned_events(spark: SparkSession, input_path: str):
    return spark.read.parquet(input_path)


def derive_sessions(df):
    """
    Recompute real sessions from event timestamps, rather than trusting
    the session_id already present in the data. This is the standard
    clickstream sessionization technique:

      1. For each user, order events by time.
      2. Compute the gap (in minutes) since that user's previous event.
      3. A gap > 30 minutes (or no previous event at all) marks the
         START of a new session.
      4. A running total of "new session" flags, per user, gives each
         session a unique sequential number.
      5. Combine user_id + session number into a single session key.
    """
    user_time_window = Window.partitionBy("user_id").orderBy("event_ts")

    with_gaps = df.withColumn(
        "prev_event_ts", F.lag("event_ts").over(user_time_window)
    ).withColumn(
        "minutes_since_prev",
        (F.col("event_ts").cast("long") - F.col("prev_event_ts").cast("long")) / 60.0,
    )

    with_session_flag = with_gaps.withColumn(
        "is_new_session",
        F.when(
            F.col("prev_event_ts").isNull() | (F.col("minutes_since_prev") > SESSION_GAP_MINUTES),
            1,
        ).otherwise(0),
    )

    # Running count of "new session" flags per user = that user's
    # sequential session number (1, 2, 3, ...).
    with_session_num = with_session_flag.withColumn(
        "session_num",
        F.sum("is_new_session").over(
            user_time_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ),
    )

    return with_session_num.withColumn(
        "derived_session_key", F.concat_ws("_", "user_id", "session_num")
    )


def build_sessions_table(df):
    """One row per derived session: duration, event count, pages
    viewed, and whether it converted to a purchase."""
    return (
        df.groupBy("derived_session_key", "user_id")
        .agg(
            F.min("event_ts").alias("session_start"),
            F.max("event_ts").alias("session_end"),
            F.count("*").alias("num_events"),
            F.countDistinct("page_url").alias("pages_viewed"),
            F.max(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("converted"),
        )
        .withColumn(
            "duration_seconds",
            F.col("session_end").cast("long") - F.col("session_start").cast("long"),
        )
        .withColumn("dt", F.to_date("session_start"))
    )


def build_daily_metrics_table(events_df, sessions_df):
    """One row per day: DAU, session counts/duration, and funnel
    counts + conversion rates across the event-type stages."""
    events_with_dt = events_df.withColumn("dt", F.to_date("event_ts"))

    daily_active_users = events_with_dt.groupBy("dt").agg(
        F.countDistinct("user_id").alias("daily_active_users")
    )

    daily_session_stats = sessions_df.groupBy("dt").agg(
        F.count("*").alias("total_sessions"),
        F.avg("duration_seconds").alias("avg_session_duration_seconds"),
        F.avg("pages_viewed").alias("avg_pages_per_session"),
        F.sum("converted").alias("converting_sessions"),
    )

    # Funnel: count of sessions reaching each stage. A session "reaches"
    # a stage if it contains at least one event of that type, letting
    # us compute stage-over-stage drop-off. sessions_df already
    # supplies "dt" here, so no second join is needed for it.
    funnel_counts = sessions_df.join(
        events_with_dt.select("derived_session_key", "event_type").distinct(),
        on="derived_session_key",
        how="left",
    )
    funnel_by_day = funnel_counts.groupBy("dt").agg(
            F.countDistinct(
                F.when(F.col("event_type") == "page_view", F.col("derived_session_key"))
            ).alias("sessions_with_page_view"),
            F.countDistinct(
                F.when(F.col("event_type") == "click", F.col("derived_session_key"))
            ).alias("sessions_with_click"),
            F.countDistinct(
                F.when(F.col("event_type") == "add_to_cart", F.col("derived_session_key"))
            ).alias("sessions_with_add_to_cart"),
            F.countDistinct(
                F.when(F.col("event_type") == "purchase", F.col("derived_session_key"))
            ).alias("sessions_with_purchase"),
        )

    daily = (
        daily_active_users.join(daily_session_stats, on="dt", how="outer")
        .join(funnel_by_day, on="dt", how="outer")
        .withColumn(
            "purchase_conversion_rate",
            F.when(
                F.col("sessions_with_page_view") > 0,
                F.col("sessions_with_purchase") / F.col("sessions_with_page_view"),
            ).otherwise(F.lit(0.0)),
        )
    )

    return daily


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    configure_writes(spark)

    events_df = read_cleaned_events(spark, args.input_path)
    events_df.cache()  # reused across multiple aggregations below

    sessionized_df = derive_sessions(events_df)
    sessionized_df.cache()  # reused for both output tables

    sessions_table = build_sessions_table(sessionized_df)
    daily_metrics_table = build_daily_metrics_table(sessionized_df, sessions_table)

    session_count = sessions_table.count()
    print(f"[sessionize_events] Derived {session_count} sessions from {events_df.count()} events")

    sessions_table.write.mode("overwrite").partitionBy("dt").parquet(
        f"{args.output_path}/sessions"
    )
    daily_metrics_table.write.mode("overwrite").parquet(f"{args.output_path}/daily_metrics")

    print(f"[sessionize_events] Wrote sessions and daily_metrics to {args.output_path}")

    # Print a quick preview so this is useful to look at directly in
    # Airflow task logs, not just when reading the Parquet files later.
    print("[sessionize_events] Daily metrics preview:")
    daily_metrics_table.orderBy("dt").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
