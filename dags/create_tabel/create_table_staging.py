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
    'create_stg_table',
    default_args=default_args,
    schedule_interval='@once',
)


def create_stg(collection, base):
    task = PostgresOperator(
        dag=dag,
        task_id=f'create_stg_{base}_{collection}',
        postgres_conn_id='postgres_localhost',
        sql=f"""
                CREATE SEQUENCE IF NOT EXISTS {base}_{collection}_id_seq
                  START WITH 1
                  INCREMENT BY 1
                  NO MINVALUE
                  NO MAXVALUE
                  CACHE 1;
                
                 
                CREATE TABLE IF NOT EXISTS staging_{base}_{collection} (
                    id integer NOT NULL DEFAULT nextval('{base}_{collection}_id_seq') PRIMARY KEY, 
                    table_name VARCHAR(200), 
                    time TIMESTAMP,
                    values json
                );"""
    )
    return task


task_mongo = [create_stg(collection, 'mng') for collection in ['clients', 'orders', 'restaurants']]
task_pstgres = [create_stg(collection, 'pstg') for collection in ['category', 'client', 'dish', 'payment']]
task_api = [create_stg(collection, 'api') for collection in ['delivery', 'deliveryman']]
task_mongo
task_pstgres
task_api
