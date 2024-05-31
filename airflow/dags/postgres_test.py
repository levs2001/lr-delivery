from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_with_postgres_operator_v03',
    default_args=default_args,
    schedule_interval='@once',
)


task1 = PostgresOperator(
        dag=dag,
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists test (
                test_id VARCHAR(10),
                dt date
            )
        """
        )

task1
