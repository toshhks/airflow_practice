from datetime import datetime, timedelta
import os

import pandas as pd
import re
import string
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.datasets import Dataset
from airflow.utils.task_group import TaskGroup


INCOMING_FILE = "/opt/airflow/data/incoming/tiktok_google_play_reviews.csv"
NULLS_FILLED_FILE = "/opt/airflow/data/processed/tiktok_reviews_nulls_filled.csv"
SORTED_FILE = "/opt/airflow/data/processed/tiktok_reviews_sorted.csv"
CLEAN_FILE = "/opt/airflow/data/processed/tiktok_reviews_clean.csv"
processed_reviews = Dataset(f"file://{CLEAN_FILE}")

def replace_nulls() -> None:
    df = pd.read_csv(INCOMING_FILE)
    df = df.fillna("-").replace("null", "-").replace(r"^\s*$", "-", regex=True)
    df.to_csv(NULLS_FILLED_FILE, index=False)

def sort_by_at() -> None:
    df = pd.read_csv(NULLS_FILLED_FILE)
    df['at'] = pd.to_datetime(df['at'], errors='coerce')
    df = df.sort_values('at')
    df.to_csv(SORTED_FILE, index=False)

def clean_content() -> None:
    junk = re.compile(rf"[^\w\s{re.escape(string.punctuation)}]+", flags=re.UNICODE)
    df = pd.read_csv(SORTED_FILE)
    df["content"] = (
        df["content"].fillna("-").astype(str).str.replace(junk, "", regex=True)
    )
    df["content"] = df["content"].replace(r"^\s*$", "-", regex=True)
    df.to_csv(CLEAN_FILE, index=False)

def choose_branch() -> str:
    if not os.path.exists(INCOMING_FILE) or os.path.getsize(INCOMING_FILE) == 0:
        return "log_empty_file"

    df = pd.read_csv(INCOMING_FILE, nrows=1)
    if df.empty:
        return "log_empty_file"

    return "process_data.replace_nulls"


with DAG(
    dag_id="process_tiktok_reviews",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["tiktok", "reviews"],
    doc_md="Wait for CSV, then branch: empty file vs processing.",
) as dag:
    wait_for_file = FileSensor(
        task_id="wait_for_review_file",
        filepath=INCOMING_FILE,
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=60 * 60,
        mode="poke",
    )

    branch = BranchPythonOperator(
        task_id="check_if_empty",
        python_callable=choose_branch,
    )

    log_empty_file = BashOperator(
        task_id="log_empty_file",
        bash_command="bash /opt/airflow/scripts/log_empty.sh ",
    )

    with TaskGroup(group_id="process_data") as process_data:
        t1 = PythonOperator(task_id="replace_nulls", python_callable=replace_nulls)
        t2 = PythonOperator(task_id="sort_by_at", python_callable=sort_by_at)
        t3 = PythonOperator(
            task_id="clean_content",
            python_callable=clean_content,
            outlets=[processed_reviews],
        )
        t1 >> t2 >> t3

    wait_for_file >> branch >> [log_empty_file, process_data]
