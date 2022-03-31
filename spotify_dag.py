from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from practice2 import spotify_etl_func
from airflow.utils.dates import days_ago



with DAG(
    dag_id='Spotify_dag',
    schedule_interval='* * * * *',
    start_date=datetime(year=2022, month=2, day=1),
    catchup=False
) as dag:


    # HERE IS THE DAG!!!
    run_etl = PythonOperator(
        task_id='spotify_etl_postgresql',
        python_callable = spotify_etl_func

    )


run_etl