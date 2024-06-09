from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
import json
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 3, 16),
}

dag = DAG(
    'unload_dds_to_dm',
    default_args=default_args,
    schedule_interval='@once',
)

db_params = {
    'dbname': 'test',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'postgres',
    'port': '5432'
}


def get_data_from_postgres(sql_query, db_params):
    """Возвращает данные из PostgreSQL в виде списка кортежей."""

    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    cur.execute(sql_query)
    data = cur.fetchall()

    column_names = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()

    return data, column_names


sql_query = """with flat_table as (
        select 	dm.deliveryman_id, name, d.delivery_id, o.order_id, rating,
                tips, o.client_id, cost, EXTRACT(MONTH FROM date(order_date)) as ord_month,
        CASE
                WHEN rating < 8 THEN GREATEST(cost  * 0.05, 400) 
                WHEN rating >= 10 THEN LEAST(cost * 0.1, 1000)
                ELSE NULL
            end as deliveryman_order_income 
        from dds_deliveryman as dm 
        join dds_delivery as d on dm.deliveryman_id = d.deliveryman_id 
        join dds_orders as o on o.order_id = d.order_id
        )
    
    
    select  deliveryman_id, name, ord_month, count(delivery_id) as orders_amount, sum (cost) as orders_total_cost, 
            cast(AVG(rating) as FLOAT) as avg_rating, sum(cost) * 0.5  as company_commission,
            SUM(deliveryman_order_income) as sum_income, sum(tips) as tips
    from flat_table
    group by deliveryman_id, name, ord_month
    order by deliveryman_id;"""


def pipeline(sql_query=sql_query, db_params=db_params):
    data, column_names = get_data_from_postgres(sql_query, db_params)
    df = pd.DataFrame(data, columns=column_names)
    conn = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/test')
    df.to_sql('dm_delivery_calculation', con=conn, if_exists='replace', index=False)


unload_task = PythonOperator(
    task_id=f'unload_dds_to_dm',
    python_callable=pipeline,
    op_args=[sql_query, db_params],
    dag=dag
)
unload_task
