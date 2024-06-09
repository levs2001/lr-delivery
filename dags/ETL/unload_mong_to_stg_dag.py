from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 3, 16),
}

dag = DAG(
    'unload_mong_to_stg_dag',
    default_args=default_args,
    schedule_interval='@once',
)


def unload_mong_to_stg(collection_name):
    import psycopg2
    from pymongo import MongoClient
    from datetime import datetime, timezone
    import json

    def get_mongo_data(collection_name, cursor_date, url='1.mongo.lr:30001', schema='restaurant_db'):
        """Выгружаем данные из mongo в виде списка словарей."""

        client = MongoClient(url)
        db = client[schema]
        collection = db[collection_name]

        cursor_date = datetime.fromisoformat(cursor_date).replace(tzinfo=timezone.utc)
        data = list(collection.find({'update_time': {'$gt': cursor_date}}))
        for row in data:
            row['_id'] = str(row['_id'])
            row['update_time'] = row['update_time'].isoformat()

        return data

    def upload_mongo_to_postrges(data, table_name, db_params):
        """ Загружаем данные в staging. """
        time = datetime.now()
        conn = psycopg2.connect(**db_params)

        for row in data:
            values_json = json.dumps(row, ensure_ascii=False)
            cur = conn.cursor()
            cur.execute(
                f"INSERT INTO staging_mng_{table_name} (table_name, time, values) VALUES (%s, %s, %s)",
                (table_name, time, values_json)
            )
            conn.commit()
            cur.close()
        conn.close()

    db_params = {
        'dbname': 'test',
        'user': 'airflow',
        'password': 'airflow',
        'host': 'postgres',
        'port': '5432'
    }

    data = get_mongo_data(collection_name=collection_name, cursor_date='2020-01-01 09:00:00')
    upload_mongo_to_postrges(data=data, table_name=collection_name, db_params=db_params)


def get_operator(collection):
    unload_task = PythonOperator(
        task_id=f'unload_task_{collection}',
        python_callable=unload_mong_to_stg,
        op_args=[collection],
        dag=dag
    )
    return unload_task


tasks = [get_operator(col) for col in ['clients', 'orders', 'restaurants']]