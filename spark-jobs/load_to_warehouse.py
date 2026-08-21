"""
Phase 6 - Gold to warehouse: load the aggregated tables into Postgres.

Reads the sessions and daily_metrics Parquet tables (from
sessionize_events.py) and writes them into a Postgres warehouse via
JDBC, so a BI tool like Metabase can query them directly with SQL.

This job fully recomputes both tables on every run (mode="overwrite"),
matching the rest of the pipeline's "full recompute" pattern rather
than incremental upserts - simple and correct for this project's
scale, at the cost of redoing more work than strictly necessary as
data grows. A natural stretch goal is switching this to incremental
loads keyed by "dt".

Usage (inside the spark-master container):
    spark-submit \
        --jars /opt/spark/custom-jars/postgresql-42.7.4.jar \
        /opt/spark/jobs/load_to_warehouse.py \
        --input-path /opt/spark/data/aggregated \
        --jdbc-url jdbc:postgresql://warehouse-postgres:5432/warehouse \
        --db-user warehouse \
        --db-password warehouse
"""

import argparse

from pyspark.sql import SparkSession


def build_spark_session(app_name: str = "load_to_warehouse") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def load_table(spark: SparkSession, parquet_path: str, jdbc_url: str, table_name: str, db_user: str, db_password: str):
    df = spark.read.parquet(parquet_path)
    row_count = df.count()

    (
        df.write.mode("overwrite")
        .jdbc(
            url=jdbc_url,
            table=table_name,
            properties={
                "user": db_user,
                "password": db_password,
                "driver": "org.postgresql.Driver",
            },
        )
    )

    print(f"[load_to_warehouse] Loaded {row_count} rows into '{table_name}'")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True, help="Root of the aggregated/ folder")
    parser.add_argument("--jdbc-url", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-password", required=True)
    args = parser.parse_args()

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    load_table(
        spark,
        f"{args.input_path}/sessions",
        args.jdbc_url,
        "fact_sessions",
        args.db_user,
        args.db_password,
    )
    load_table(
        spark,
        f"{args.input_path}/daily_metrics",
        args.jdbc_url,
        "fact_daily_metrics",
        args.db_user,
        args.db_password,
    )

    spark.stop()


if __name__ == "__main__":
    main()
