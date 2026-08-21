"""
Phase 3 - Bronze to Silver: Clean raw clickstream events.

Reads newline-delimited JSON events from the raw zone, validates and
deduplicates them, and writes the result as partitioned Parquet to the
processed zone.

This is meant to be run via spark-submit, either manually or (later)
triggered by an Airflow SparkSubmitOperator.

Usage (inside the spark-master container):
    spark-submit /opt/bitnami/spark/jobs/clean_events.py \
        --input-path /opt/bitnami/spark/data/raw \
        --output-path /opt/bitnami/spark/data/processed
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


VALID_EVENT_TYPES = ["page_view", "click", "add_to_cart", "purchase"]


def build_spark_session(app_name: str = "clean_events") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def read_raw_events(spark: SparkSession, input_path: str):
    # Spark auto-discovers the dt=/hour= partition folders and exposes
    # them as columns automatically if we point it at the root path.
    df = spark.read.json(f"{input_path}/*/*/*.json")
    return df


def clean_events(df):
    """
    Cleaning rules:
      1. Drop rows with null event_id, user_id, or timestamp - these
         are unusable for downstream aggregation.
      2. Drop rows with an event_type outside our known set - guards
         against upstream data quality issues.
      3. Deduplicate by event_id - a real event stream can deliver the
         same event more than once (at-least-once delivery is common),
         so we must dedupe to keep counts/aggregates correct.
      4. Cast timestamp string to a proper timestamp type.
    """
    before_count = df.count()

    cleaned = (
        df.filter(
            F.col("event_id").isNotNull()
            & F.col("user_id").isNotNull()
            & F.col("timestamp").isNotNull()
        )
        .filter(F.col("event_type").isin(VALID_EVENT_TYPES))
        .withColumn("event_ts", F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    )

    # Deduplicate: keep one row per event_id. Using a window function
    # instead of dropDuplicates() so we deterministically keep the
    # first-seen row rather than an arbitrary one.
    window = Window.partitionBy("event_id").orderBy(F.col("event_ts").asc())
    cleaned = (
        cleaned.withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    after_count = cleaned.count()
    dropped = before_count - after_count
    print(f"[clean_events] Read {before_count} raw rows, kept {after_count}, dropped {dropped}")

    return cleaned


def add_partition_columns(df):
    # Derive dt and hour from the actual event timestamp (not the folder
    # it happened to land in) so partitions reflect real event time.
    return df.withColumn("dt", F.to_date("event_ts")).withColumn(
        "hour", F.date_format("event_ts", "HH")
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    args = parser.parse_args()

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # quiet down Spark's noisy default logging

    # IMPORTANT: with the default "static" overwrite mode, Spark deletes
    # the ENTIRE output directory before writing - including the
    # directory itself. When output-path is a Docker bind-mount root
    # (as it is here), that delete fails with "Device or resource busy"
    # because you can't remove a mount point from inside the container.
    # "dynamic" mode instead only deletes the specific dt=/hour=
    # partition folders being overwritten, leaving the mount-root
    # directory itself untouched.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # Hadoop's default ("v1") output commit algorithm stages all task
    # output under a job-wide _temporary directory, then does a single
    # directory-level rename into the final location when the whole job
    # completes. That kind of whole-directory rename is unreliable on
    # Windows/WSL2 Docker bind mounts. "v2" instead has each task commit
    # its own output file directly to its final path as soon as that
    # task finishes - only per-file renames, no directory-level rename.
    spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")

    raw_df = read_raw_events(spark, args.input_path)
    cleaned_df = clean_events(raw_df)
    partitioned_df = add_partition_columns(cleaned_df)

    (
        partitioned_df.write.mode("overwrite")
        .partitionBy("dt", "hour")
        .parquet(args.output_path)
    )

    print(f"[clean_events] Wrote cleaned data to {args.output_path}")
    spark.stop()


if __name__ == "__main__":
    main()
