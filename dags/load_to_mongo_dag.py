from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

CLEAN_FILE="/opt/airflow/data/processed/tiktok_reviews_clean.csv"
processed_reviews = Dataset(f'file://{CLEAN_FILE}')
BATCH_SIZE = 1000

def load_to_mongo() -> None:
    df = pd.read_csv(CLEAN_FILE)
    docs = df.to_dict(orient='records')

    hook = MongoHook(conn_id='mongo_default')
    collection = hook.get_collection('reviews', mongo_db='airflow_data')
    collection.delete_many({})

    for i in range(0, len(docs), BATCH_SIZE):
        collection.insert_many(docs[i:i+BATCH_SIZE])

with DAG(
    dag_id='load_to_mongo_dag',
    start_date=datetime(2026, 1, 1),
    schedule=[processed_reviews],
    catchup=False,
    default_args={
        "retries":1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=['tiktok', 'reviews', 'mongo'],
    doc_md="Load cleaned TikTok reviews into MongoDB when the Dataset updates.",
) as dag:
    PythonOperator(task_id='load_to_mongo', python_callable=load_to_mongo)