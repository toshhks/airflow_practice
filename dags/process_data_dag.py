from datetime import datetime, timedelta
import os

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.filesystem import FileSensor

INCOMING_FILE = "/opt/airflow/data/incoming/tiktok_google_play_reviews.csv"


def choose_branch() -> str:
    if not os.path.exists(INCOMING_FILE) or os.path.getsize(INCOMING_FILE) == 0:
        return "log_empty_file"

    df = pd.read_csv(INCOMING_FILE, nrows=1)
    if df.empty:
        return "log_empty_file"

    return "process_data"


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

    process_data = EmptyOperator(task_id="process_data")

    wait_for_file >> branch >> [log_empty_file, process_data]