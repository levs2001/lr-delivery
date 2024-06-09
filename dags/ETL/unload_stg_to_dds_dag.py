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
    'unload_stg_to_dds',
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


def get_stg_data(table, cursor_date):
    sql_query = f"""
                   SELECT values 
                   FROM {table}
                   WHERE time > '{cursor_date}'
                """
    data, column_names = get_data_from_postgres(sql_query, db_params)
    data = [row[0] for row in data]
    return data


def upload_to_dds_delivery(stg_table, dds_table, cursor_date):
    data = get_stg_data(stg_table, cursor_date)
    df = pd.DataFrame(data)
    conn = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/test')
    df.to_sql(dds_table, con=conn, if_exists='append', index=False)


def upload_restuarn_menu(cursor_date):
    # ----restuarn-----
    data = get_stg_data('staging_mng_restaurants', cursor_date)
    conn = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/test')
    df = pd.DataFrame(data)
    select_cols = [i for i in df.columns if i not in ['_id', 'menu']]
    df = df[select_cols]
    df.to_sql('dds_restaurants', con=conn, if_exists='append', index=False)

    # ----menu-----
    menu = []
    for row in data:
        for i in row['menu']:
            menu.append(i)

    df = pd.DataFrame(menu)
    select_cols = [i for i in df.columns if i not in ['id']]
    df = df[select_cols]
    df.to_sql('dds_menu', con=conn, if_exists='append', index=False)

    # ----menu-_resturan----
    rest_list = []
    menu_list = []
    for row in data:
        r_id = row['restaurant_id']
        for m_row in row['menu']:
            rest_list.append(r_id)
            menu_list.append(m_row['menu_id'])

    df = pd.DataFrame()
    df['restaurant_id'] = rest_list
    df['menu_id'] = menu_list
    df.to_sql('dds_menu_restauran', con=conn, if_exists='append', index=False)


def upload_orders_dish_status(cursor_date):
    conn = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/test')

    # -----oreders-------
    data = get_stg_data('staging_mng_orders', cursor_date)
    df = pd.DataFrame(data)
    select_cols = [i for i in df.columns if i not in ['_id', 'statuses', 'ordered_dish']]
    df = df[select_cols]
    df.to_sql('dds_orders', con=conn, if_exists='append', index=False)

    # -----dish-------
    dish = []
    for row in data:
        for i in row['ordered_dish']:
            dish.append(i)

    df = pd.DataFrame(dish)
    select_cols = [i for i in df.columns if i not in ['id']]
    df = df[select_cols]
    df.to_sql('dds_dish', con=conn, if_exists='append', index=False)

    # -----dds_orders_dish-------
    order_list = []
    dish_list = []
    for row in data:
        r_id = row['order_id']
        for m_row in row['ordered_dish']:
            order_list.append(r_id)
            dish_list.append(m_row['dish_id'])

    df = pd.DataFrame()
    df['order_id'] = order_list
    df['dish_id'] = dish_list
    df.to_sql('dds_orders_dish', con=conn, if_exists='append', index=False)

    # -----dds_status-------
    status = []
    for row in data:
        for i in row['statuses']:
            status.append(i)

    df = pd.DataFrame(status)
    df.to_sql('dds_status', con=conn, if_exists='append', index=False)

    # -----dds_status_orders-------
    order_list = []
    status_list = []
    for row in data:
        r_id = row['order_id']
        for m_row in row['statuses']:
            order_list.append(r_id)
            status_list.append(m_row['status_id'])

    df = pd.DataFrame()
    df['order_id'] = order_list
    df['status_id'] = status_list
    df.to_sql('dds_status_orders', con=conn, if_exists='append', index=False)


def upload_colient_category(cursor_date):
    conn = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/test')

    # -----clients----
    data = get_stg_data('staging_mng_clients', cursor_date)
    df = pd.DataFrame(data)
    select_cols = [i for i in df.columns if i not in ['_id', 'statuses', 'ordered_dish']]
    df = df[select_cols]
    data = get_stg_data('staging_pstg_client', cursor_date)
    df_client = pd.DataFrame(data)
    df = df.merge(df_client, on='client_id', how='left')
    df['birthday'] = df['birthday'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').strftime('%Y-%m-%d'))
    df.to_sql('dds_clients', con=conn, if_exists='append', index=False)

    # -----category----
    data = get_stg_data('staging_pstg_category', cursor_date)
    df = pd.DataFrame(data)
    df.to_sql('dds_category', con=conn, if_exists='append', index=False)





def pipeline(cursor_date):
    upload_to_dds_delivery('staging_api_deliveryman', 'dds_deliveryman', cursor_date)
    upload_to_dds_delivery('staging_api_delivery', 'dds_delivery', cursor_date)
    upload_restuarn_menu(cursor_date)
    upload_orders_dish_status(cursor_date)
    upload_colient_category(cursor_date)

cursor_date = '2020-01-01 09:00:00'
unload_task = PythonOperator(
        task_id=f'unload_task_to_dds',
        python_callable=pipeline,
        op_args=[cursor_date],
        dag=dag
    )
unload_task
