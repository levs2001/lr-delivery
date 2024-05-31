from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'print_time_dag',
    default_args=default_args,
    schedule_interval='@once',
)

def print_time():
    """
    Prints the current time to the Airflow logs.
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'Current time: {now}')

print_time_task = PythonOperator(
    task_id='print_time_task',
    python_callable=print_time,
    dag=dag,
)
