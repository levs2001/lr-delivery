from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 3, 16),
}

dag = DAG(
    'unload_api_to_stg_dag',
    default_args=default_args,
    schedule_interval='@once',
)


def upload_api_to_postrges(data, table_name, db_params):
    """ Загружаем данные в staging. """
    time = datetime.now()
    conn = psycopg2.connect(**db_params)

    for row in data:
        values_json = json.dumps(row, ensure_ascii=False)
        cur = conn.cursor()
        cur.execute(
            f"INSERT INTO staging_api_{table_name} (table_name, time, values) VALUES (%s, %s, %s)",
            (table_name, time, values_json)
        )
        conn.commit()
    cur.close()
    conn.close()


def unload_api_to_stg(table, cursor_date='2024-01-01 09:00:00'):
    db_params = {
        'dbname': 'test',
        'user': 'airflow',
        'password': 'airflow',
        'host': 'postgres',
        'port': '5432'
    }

    with open(f'/opt/airflow/dags/ETL/api/{table}.json', 'r') as file:
        data = json.load(file)

    match_time_col = {'deliveryman': 'update_time', 'delivery': 'order_date_created'}
    time_feature = match_time_col[table]

    upload_data = []

    for row in data:
        if row[time_feature] > cursor_date:
            upload_data.append(row)

    if len(upload_data) == 0:
        print('No actual data')
        return
    upload_api_to_postrges(upload_data, table, db_params)


def get_operator(table):
    unload_task = PythonOperator(
        task_id=f'unload_task_{table}',
        python_callable=unload_api_to_stg,
        op_args=[table],
        dag=dag
    )
    return unload_task


tasks = [get_operator(api_endpoint) for api_endpoint in ['delivery', 'deliveryman']]
