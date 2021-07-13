import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor

default_args = {"owner": "airflow"}

with DAG(
    dag_id="product_staging",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    check_product_csv = BashSensor(
        task_id="check_product_csv",
        bash_command="""
            ls /data/raw/products_{{ ds }}.csv
        """,
    )

    import_product_to_staging = BashOperator(
        task_id="import_product_to_staging",
        bash_command='cp /data/raw/products_{{ ds }}.csv /data/stg/',
    )

    check_product_csv >> import_product_to_staging
