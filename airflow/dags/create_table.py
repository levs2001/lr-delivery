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
    'create_pstg_table',
    default_args=default_args,
    schedule_interval='@once',
)


task1 = PostgresOperator(
        dag=dag,
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS client (
            client_id INT PRIMARY KEY, 
            bonus_balance FLOAT, 
            category_id INT 
        );
        
        CREATE TABLE IF NOT EXISTS category (
            category_id INT PRIMARY KEY, 
            name VARCHAR(200), 
            percent FLOAT, 
            min_payment FLOAT
        );
        
        CREATE TABLE IF NOT EXISTS payment (
            payment_id INT PRIMARY KEY, 
            client_id INT, 
            dish_id INT, 
            dish_amount FLOAT,
            order_id INT,
            order_time TIMESTAMP,
            order_sum FLOAT,
            tips FLOAT
        );
        
        CREATE TABLE IF NOT EXISTS dish (
            dish_id INT PRIMARY KEY, 
            name VARCHAR(200), 
            price FLOAT
        );
        
        
        CREATE TABLE IF NOT EXISTS logs (
            id INT PRIMARY KEY, 
            table_name VARCHAR(200), 
            time TIMESTAMP,
            values VARCHAR(10000) 
        );
        """
        )

task1
