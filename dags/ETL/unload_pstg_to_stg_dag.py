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
    'unload_pstg_to_stg_dag',
    default_args=default_args,
    schedule_interval='@once',
)


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


def upload_pstg_to_postrges(data, table_name, db_params):
    """ Загружаем данные в staging. """
    time = datetime.now()
    conn = psycopg2.connect(**db_params)

    for row in data:
        values_json = json.dumps(row, ensure_ascii=False)
        cur = conn.cursor()
        cur.execute(
            f"INSERT INTO staging_pstg_{table_name} (table_name, time, values) VALUES (%s, %s, %s)",
            (table_name, time, values_json)
        )
        conn.commit()
    cur.close()
    conn.close()


# def get_cursor_date(db_params, table, time_feature):
#     conn = psycopg2.connect(**db_params)
#     cur = conn.cursor()
#     sql_query = f"""
#                SELECT cursor_time
#                FROM cursor_timestamp
#                WHERE table_name = '{table}'
#             """
#     cur.execute(sql_query)
#     data = cur.fetchall()
#     curs_time = str(data[0][0])
#     cur.close()
#
#     sql_query = f"""
#                SELECT {time_feature}
#                FROM {table}
#                WHERE {time_feature} = (SELECT max({time_feature}) FROM {table});
#             """
#     cur.execute(sql_query)
#     data = cur.fetchall()
#     max_date = str(data[0][0])
#     cur.close()
#
#     max_date = max(max_date, curs_time)
#
#     sql_query = f"""
#                UPDATE cursor_timestamp
#                SET cursor_time = '{max_date}'
#                WHERE table_name = '{table}';
#             """
#     cur.execute(sql_query)
#     cur.close()
#     return max_date

def unload_pstg_to_stg(table, cursor_date = '2024-01-01 09:00:00'):
    db_params = {
        'dbname': 'test',
        'user': 'airflow',
        'password': 'airflow',
        'host': 'postgres',
        'port': '5432'
    }
    #cursor_date = get_cursor_date(db_params, table, 'time')

    sql_query = f"""
                   SELECT values 
                   FROM logs
                   WHERE table_name = '{table}' AND time > '{cursor_date}'
                """
    data, column_names = get_data_from_postgres(sql_query, db_params)
    data = [row[0] for row in data]

    if len(data) == 0:
        print('No actual data')
        return
    upload_pstg_to_postrges(data, table, db_params)


def get_operator(table):
    unload_task = PythonOperator(
        task_id=f'unload_task_{table}',
        python_callable=unload_pstg_to_stg,
        op_args=[table],
        dag=dag
    )
    return unload_task


tasks = [get_operator(col) for col in ['category', 'client', 'dish', 'payment']]
