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
        CREATE TABLE IF NOT EXISTS cursor_timestamp (
             table_name VARCHAR(100) UNIQUE,
             cursor_time TIMESTAMP 
        ); 
        
        INSERT INTO cursor_timestamp (table_name, cursor_time)
        VALUES
          ('logs', '2022-01-01 00:00:00'),
          ('delivery', '2022-01-01 00:00:00'),
          ('deliveryman', '2022-01-01 00:00:00'),
          ('clients', '2022-01-01 00:00:00'),
          ('orders', '2022-01-01 00:00:00'),
          ('restaurants', '2022-01-01 00:00:00'),
          ('staging_api_delivery', '2022-01-01 00:00:00'),
          ('staging_api_deliveryman', '2022-01-01 00:00:00'),
          ('staging_mng_clients', '2022-01-01 00:00:00'),
          ('staging_mng_orders', '2022-01-01 00:00:00'),
          ('staging_mng_restaurants', '2022-01-01 00:00:00'),
          ('staging_pstg_category', '2022-01-01 00:00:00'),
          ('staging_pstg_client', '2022-01-01 00:00:00'),
          ('staging_pstg_dish', '2022-01-01 00:00:00'),
          ('staging_pstg_payment', '2022-01-01 00:00:00');
        
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
            values json
        );
        """
        )

task1
