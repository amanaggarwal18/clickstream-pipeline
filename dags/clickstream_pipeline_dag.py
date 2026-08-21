import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Both tasks need these - avoiding the two Windows-bind-mount issues we
# hit earlier (whole-directory delete on overwrite, whole-directory
# rename on commit).
SPARK_WRITE_CONF = {
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
}

WAREHOUSE_CONN = {
    "host": "warehouse-postgres",
    "port": 5432,
    "dbname": "warehouse",
    "user": "warehouse",
    "password": "warehouse",
}


def validate_warehouse_data():
    """
    Data quality gate: runs after the load and fails the task (and
    therefore the whole DAG run) loudly if anything looks wrong,
    instead of silently shipping bad data downstream to whoever reads
    from the warehouse next (a dashboard, an analyst, another job).

    Each check below was verified to genuinely catch a real problem
    (not just always pass) before being added here.
    """
    import psycopg2

    conn = psycopg2.connect(**WAREHOUSE_CONN)
    cur = conn.cursor()
    failures = []

    # Check 1: sessions should be unique. A duplicate here would mean
    # double-counting a user's activity in every downstream metric.
    cur.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT derived_session_key FROM fact_sessions
            GROUP BY derived_session_key HAVING COUNT(*) > 1
        ) dupes;
        """
    )
    dupe_count = cur.fetchone()[0]
    if dupe_count > 0:
        failures.append(f"{dupe_count} duplicate session key(s) in fact_sessions")

    # Check 2: the funnel must narrow at each stage, never widen. A
    # violation here would mean the sessionization or funnel-counting
    # logic is broken (e.g. counting purchases that were never preceded
    # by a page view).
    cur.execute(
        """
        SELECT dt FROM fact_daily_metrics
        WHERE NOT (
            sessions_with_page_view >= sessions_with_click
            AND sessions_with_click >= sessions_with_add_to_cart
            AND sessions_with_add_to_cart >= sessions_with_purchase
        );
        """
    )
    bad_funnel_days = [row[0].isoformat() for row in cur.fetchall()]
    if bad_funnel_days:
        failures.append(f"Funnel is not monotonically decreasing on: {bad_funnel_days}")

    # Check 3: every day present should have actual active users. A
    # zero/null here likely means an upstream join produced empty rows.
    cur.execute(
        "SELECT COUNT(*) FROM fact_daily_metrics WHERE daily_active_users IS NULL OR daily_active_users <= 0;"
    )
    bad_dau_count = cur.fetchone()[0]
    if bad_dau_count > 0:
        failures.append(f"{bad_dau_count} row(s) in fact_daily_metrics have invalid daily_active_users")

    cur.close()
    conn.close()

    if failures:
        message = "Data quality checks FAILED:\n  - " + "\n  - ".join(failures)
        logging.error(message)
        raise ValueError(message)

    logging.info("All data quality checks passed.")


def alert_on_failure(context):
    """
    Minimal stand-in for real alerting (Slack/email/PagerDuty). Swap
    the body of this function for a real notification call - e.g. an
    airflow.providers.slack SlackWebhookOperator or an EmailOperator -
    once you have credentials for one of those set up. The pattern
    (attach via on_failure_callback in default_args) stays the same.
    """
    task_instance = context["task_instance"]
    logging.error(
        "ALERT: task '%s' in dag '%s' failed on run %s. Check the task logs for details.",
        task_instance.task_id,
        task_instance.dag_id,
        context["run_id"],
    )


with DAG(
    dag_id="clickstream_pipeline",
    description="Phase 7 - Full pipeline with data quality gate, daily schedule, and failure alerting",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"on_failure_callback": alert_on_failure},
    tags=["phase7", "spark"],
) as dag:

    clean_events = SparkSubmitOperator(
        task_id="clean_events",
        conn_id="spark_default",
        application="/opt/spark/jobs/clean_events.py",
        application_args=[
            "--input-path",
            "/opt/spark/data/raw",
            "--output-path",
            "/opt/spark/data/processed",
        ],
        conf=SPARK_WRITE_CONF,
        verbose=True,
    )

    sessionize_events = SparkSubmitOperator(
        task_id="sessionize_events",
        conn_id="spark_default",
        application="/opt/spark/jobs/sessionize_events.py",
        application_args=[
            "--input-path",
            "/opt/spark/data/processed",
            "--output-path",
            "/opt/spark/data/aggregated",
        ],
        conf=SPARK_WRITE_CONF,
        verbose=True,
    )

    load_to_warehouse = SparkSubmitOperator(
        task_id="load_to_warehouse",
        conn_id="spark_default",
        application="/opt/spark/jobs/load_to_warehouse.py",
        # The Postgres JDBC driver isn't part of Spark by default - this
        # ships it to both the driver and every executor for this job.
        jars="/opt/spark/custom-jars/postgresql-42.7.4.jar",
        application_args=[
            "--input-path",
            "/opt/spark/data/aggregated",
            "--jdbc-url",
            "jdbc:postgresql://warehouse-postgres:5432/warehouse",
            "--db-user",
            "warehouse",
            "--db-password",
            "warehouse",
        ],
        verbose=True,
    )

    validate_data = PythonOperator(
        task_id="validate_warehouse_data",
        python_callable=validate_warehouse_data,
    )

    # Strict linear order: sessionizing needs cleaned data, loading
    # needs the aggregated tables sessionizing produces, and validation
    # needs the loaded warehouse tables to actually check.
    clean_events >> sessionize_events >> load_to_warehouse >> validate_data
