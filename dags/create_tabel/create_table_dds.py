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
    'create_dds_table',
    default_args=default_args,
    schedule_interval='@once',
)

task1 = PostgresOperator(
    dag=dag,
    task_id='create_postgres_dds_table',
    postgres_conn_id='postgres_localhost',
    sql="""
        CREATE TABLE IF NOT EXISTS dds_category (
        category_id INT PRIMARY key,
        name VARCHAR(200),
        percent FLOAT,
        min_payment FLOAT
    );
    
    
    CREATE TABLE IF NOT EXISTS dds_restaurants  (
        restaurant_id  INT PRIMARY key,
        name VARCHAR(150),
        phone VARCHAR(15),
        email  VARCHAR(80),
        founding_day DATE,
        update_time TIMESTAMP
    );
    
    
    CREATE TABLE IF NOT EXISTS dds_menu  (
        menu_id INT PRIMARY key,
        name VARCHAR(150),
        price FLOAT,
        dish_category VARCHAR(150)
    );
    
    
    CREATE TABLE IF NOT EXISTS dds_menu_restauran  (
        menu_id INT,
        restaurant_id INT,
        PRIMARY KEY (menu_id, restaurant_id)
    );
    
    CREATE TABLE IF NOT EXISTS dds_deliveryman (
        deliveryman_id INT PRIMARY key,
        name VARCHAR(100),
        update_time TIMESTAMP 
    );
    
    
    CREATE TABLE IF NOT EXISTS dds_delivery  (
        delivery_id INT PRIMARY key,
        order_id INT unique,
        deliveryman_id INT,
        order_date_created TIMESTAMP,
        delivery_address VARCHAR(200),
        delivery_time TIMESTAMP,
        rating INT,
        tips FLOAT
    );
    
    CREATE TABLE IF NOT EXISTS dds_dish (
        dish_id INT PRIMARY key,
        name VARCHAR(150),
        price FLOAT,
        quantity INT
    ); 
    
    CREATE TABLE IF NOT EXISTS dds_clients (
        client_id INT PRIMARY key,
        category_id INT,
        name VARCHAR(200),
        phone INT,
        birthday DATE,
        email VARCHAR(200),
        login VARCHAR(200),
        address VARCHAR(400),
        bonus_balance FLOAT,
        update_time TIMESTAMP
    ); 
    
    CREATE TABLE IF NOT EXISTS dds_status (
        status_id INT PRIMARY key,
        status VARCHAR(100),
        time TIMESTAMP
    ); 
    
    
    CREATE TABLE IF NOT EXISTS dds_orders  (
        order_id INT PRIMARY key,
        restaurant_id INT,
        client_id INT,
        order_date TIMESTAMP,
        payed_by_bonuses FLOAT,
        cost FLOAT,
        payment FLOAT,
        bonus_for_visit FLOAT,
        final_status VARCHAR(100),
        update_time TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS dds_payment (
        payment_id INT PRIMARY key,
        client_id INT,
        order_id INT,
        dish_amount FLOAT,
        order_time TIMESTAMP,
        order_sum FLOAT,
        tips FLOAT
    );
    
    CREATE TABLE IF NOT EXISTS dds_status_orders  (
        status_id INT,
        order_id  INT,
        PRIMARY KEY (status_id, order_id)
    ); 
    
    CREATE TABLE IF NOT EXISTS dds_orders_dish (
        order_id INT,
        dish_id  INT,
        PRIMARY KEY (dish_id, order_id)
    ); 
        """
)

task1
