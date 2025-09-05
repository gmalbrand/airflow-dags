from datetime import datetime, timedelta
from os import remove
from tempfile import NamedTemporaryFile

from airflow.sdk import Variable, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow import DAG


ORIGIN = datetime(2022, 1, 23)
INCREMENT_VAR = "assignment_4_inc"
INCREMENT_DEFAULT = "0"
LAST_EXEC_VAR = "assignment_4_last_execution"
LAST_EXEC_DEFAULT = "2018-01-01"
GCP_CONNECTION_ID = "gcp_pagila_service_account"
GCP_BUCKET = "pagila_bucket"
GCP_OBJECT_PREFIX = "pagila/exports/payment_by_day_"
GCP_BIGQUERY_TABLE = "dilizone.pagila_analytics.pagila_payments"
PG_CONNECTION_ID = "pg-pagila-database"

with DAG(
    dag_id="assignment_4",
    description="Real life scenario using PQSQL, GCP Bucket and BigQuery",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["assignment", "gma", "demo"],
    default_args={
        "queue": "faster-queue",
    },
) as dag:

    @task(task_id="get_incremental_variable", task_display_name="Get incremental loading variables", queue="default")
    def get_incremental_variable():
        increment = Variable.get(INCREMENT_VAR, default=INCREMENT_DEFAULT)
        if increment == INCREMENT_DEFAULT:
            Variable.set(INCREMENT_VAR, INCREMENT_DEFAULT)
        last_execution = Variable.get(LAST_EXEC_VAR, default=LAST_EXEC_DEFAULT)
        if last_execution == LAST_EXEC_DEFAULT:
            Variable.set(LAST_EXEC_VAR, LAST_EXEC_DEFAULT)

    @task(task_id="update_incremental_variable", task_display_name="Update incremental loading variables", trigger_rule="all_success", queue="default")
    def update_incremental_variable():
        Variable.set(INCREMENT_VAR, str(int(Variable.get(INCREMENT_VAR)) + 1))
        Variable.set(LAST_EXEC_VAR, "{{ ds }}")

    @task(task_id="export_to_gcs", task_display_name="Export Daily data to GCS Bucket")
    def export_to_gcs() -> str:

        pg = PostgresHook(postgres_conn_id=PG_CONNECTION_ID)
        gcs = GCSHook(gcp_conn_id=GCP_CONNECTION_ID)
        increment = int(Variable.get(INCREMENT_VAR))
        run_day = (ORIGIN + timedelta(days=increment)).strftime("%Y-%m-%d")

        extract_query = f"""
COPY(
SELECT
  p.payment_id,
  p.customer_id,
  p.staff_id,
  p.rental_id,
  p.amount,
  p.payment_date::date AS payment_date,
  s.store_id,
  f.film_id,
  c.name AS category_name
FROM payment p
JOIN rental r       ON r.rental_id = p.rental_id
JOIN inventory i    ON i.inventory_id = r.inventory_id
JOIN film f         ON f.film_id = i.film_id
JOIN film_category fc ON fc.film_id = f.film_id
JOIN category c     ON c.category_id = fc.category_id
JOIN staff st       ON st.staff_id = p.staff_id
JOIN store s        ON s.store_id = st.store_id
WHERE p.payment_date::date = '{run_day}'::date
ORDER BY p.payment_id
) TO STDOUT WITH CSV HEADER
"""
        object_name = f"{GCP_OBJECT_PREFIX}{run_day}.csv"

        with NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            pg.copy_expert(sql=extract_query, filename=tmp.name)

            gcs.upload(
                bucket_name=GCP_BUCKET,
                object_name=object_name,
                filename=tmp.name,
                mime_type="text/csv"
            )

        return object_name

    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=GCP_BUCKET,
        source_objects=["{{ ti.xcom_pull('export_to_gcs') }}"],
        destination_project_dataset_table=GCP_BIGQUERY_TABLE,
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        schema_fields=[
            {"name": "payment_id",   "type": "INT64",  "mode": "REQUIRED"},
            {"name": "customer_id",  "type": "INT64",  "mode": "REQUIRED"},
            {"name": "staff_id",     "type": "INT64",  "mode": "REQUIRED"},
            {"name": "rental_id",    "type": "INT64",  "mode": "REQUIRED"},
            {"name": "amount",       "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "payment_date", "type": "DATE",   "mode": "REQUIRED"},
            {"name": "store_id",     "type": "INT64",  "mode": "REQUIRED"},
            {"name": "film_id",      "type": "INT64",  "mode": "REQUIRED"},
            {"name": "category_name", "type": "STRING", "mode": "REQUIRED"},
        ],
        time_partitioning={"type": "DAY", "field": "payment_date"},
        gcp_conn_id=GCP_CONNECTION_ID
    )
    vars = get_incremental_variable()

    vars >> export_to_gcs() >> load_to_bq >> update_incremental_variable()
