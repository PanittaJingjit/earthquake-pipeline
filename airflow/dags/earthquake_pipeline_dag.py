from airflow import DAG
from airflow.operators.bash import (
    BashOperator
)

from datetime import datetime

default_args = {
    "owner": "airflow"
}

with DAG(

    dag_id="earthquake_pipeline",

    default_args=default_args,

    start_date=datetime(2024, 1, 1),

    schedule_interval="*/30 * * * *",

    catchup=False

) as dag:

    ingest_task = BashOperator(

        task_id="ingest_earthquake",

        bash_command=(
            "python3 "
            "/opt/airflow/scripts/"
            "ingest_earthquake.py"
        )
    )

    clean_task = BashOperator(

        task_id="clean_earthquake",

        bash_command=(
            "python3 "
            "/opt/airflow/scripts/"
            "clean_earthquake.py"
        )
    )

    transform_task = BashOperator(

        task_id="transform_earthquake",

        bash_command=(
            "python3 "
            "/opt/airflow/scripts/"
            "transform_earthquake.py"
        )
    )

    load_task = BashOperator(

        task_id="load_earthquake",

        bash_command=(
            "python3 "
            "/opt/airflow/scripts/"
            "load_earthquake.py"
        )
    )

    (
        ingest_task
        >> clean_task
        >> transform_task
        >> load_task
    )