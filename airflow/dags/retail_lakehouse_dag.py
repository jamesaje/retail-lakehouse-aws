from __future__ import annotations

from datetime import datetime, timedelta
import time

import boto3
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


AWS_REGION = "us-west-1"
GLUE_JOB_NAME = "glue-job-role-nyc-taxi"
GLUE_CRAWLER_NAME = "crawler_nyc_taxi_cleaned"

POLL_SECONDS_GLUE = 20
POLL_SECONDS_CRAWLER = 15
MAX_WAIT_SECONDS_GLUE = 60 * 30      # 30 minutes
MAX_WAIT_SECONDS_CRAWLER = 60 * 20   # 20 minutes


def _get_params(context):
    # Airflow passes params via dag params or manual trigger conf override
    year = int(context["params"]["year"])
    month = str(context["params"]["month"]).zfill(2)  # "01", "02", ...
    return year, month


def start_glue_job(**context):
    year, month = _get_params(context)
    glue = boto3.client("glue", region_name=AWS_REGION)

    resp = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--YEAR": str(year),
            "--MONTH": month,
        },
    )
    context["ti"].xcom_push(key="glue_run_id", value=resp["JobRunId"])


def wait_for_glue_job(**context):
    glue = boto3.client("glue", region_name=AWS_REGION)
    run_id = context["ti"].xcom_pull(key="glue_run_id", task_ids="start_glue_job")

    start = time.time()
    while True:
        r = glue.get_job_run(
            JobName=GLUE_JOB_NAME,
            RunId=run_id,
            PredecessorsIncluded=False,
        )
        state = r["JobRun"]["JobRunState"]

        if state in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"):
            if state != "SUCCEEDED":
                err = r["JobRun"].get("ErrorMessage", "No ErrorMessage from Glue")
                raise RuntimeError(
                    f"Glue job {GLUE_JOB_NAME} failed. state={state}, run_id={run_id}, error={err}"
                )
            return

        if time.time() - start > MAX_WAIT_SECONDS_GLUE:
            raise TimeoutError(f"Timed out waiting for Glue job {GLUE_JOB_NAME} run_id={run_id}")

        time.sleep(POLL_SECONDS_GLUE)


def start_crawler(**context):
    glue = boto3.client("glue", region_name=AWS_REGION)

    # If crawler is already running, don't error; just let the wait task handle it.
    r = glue.get_crawler(Name=GLUE_CRAWLER_NAME)
    if r["Crawler"]["State"] == "RUNNING":
        return

    glue.start_crawler(Name=GLUE_CRAWLER_NAME)


def wait_for_crawler(**context):
    glue = boto3.client("glue", region_name=AWS_REGION)

    start = time.time()
    while True:
        r = glue.get_crawler(Name=GLUE_CRAWLER_NAME)
        state = r["Crawler"]["State"]  # RUNNING / READY
        last = r["Crawler"].get("LastCrawl", {})
        status = last.get("Status")  # SUCCEEDED / FAILED (may be absent)

        if state == "READY":
            if status == "FAILED":
                raise RuntimeError(f"Crawler {GLUE_CRAWLER_NAME} finished READY but LastCrawl FAILED")
            return

        if time.time() - start > MAX_WAIT_SECONDS_CRAWLER:
            raise TimeoutError(f"Timed out waiting for crawler {GLUE_CRAWLER_NAME}")

        time.sleep(POLL_SECONDS_CRAWLER)


default_args = {
    "owner": "femi",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="retail_lakehouse_monthly",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,  # trigger manually for now
    catchup=False,
    params={"year": 2019, "month": 2},
    tags=["lakehouse", "glue", "athena", "dbt"],
) as dag:

    t1 = PythonOperator(
        task_id="start_glue_job",
        python_callable=start_glue_job,
    )

    t2 = PythonOperator(
        task_id="wait_for_glue_job",
        python_callable=wait_for_glue_job,
    )

    t3 = PythonOperator(
        task_id="start_crawler",
        python_callable=start_crawler,
    )

    t4 = PythonOperator(
        task_id="wait_for_crawler",
        python_callable=wait_for_crawler,
    )

    # Pass year/month into dbt so your incremental insert_overwrite targets that partition
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt/nyc_taxi_athena && "
            "dbt run --select stg_yellow_trips fact_trips dim_date "
            "--vars '{\"year\": {{ params.year }}, \"month\": \"{{ params.month }}\"}'"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt/nyc_taxi_athena && "
            "dbt test --no-partial-parse"
        ),
    )

    t1 >> t2 >> t3 >> t4 >> dbt_run >> dbt_test
