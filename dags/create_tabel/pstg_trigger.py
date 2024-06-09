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
    'trigger',
    default_args=default_args,
    schedule_interval='@once',
)


task1 = PostgresOperator(
        dag=dag,
        task_id='trigger_logs',
        postgres_conn_id='postgres_localhost',
        sql="""
        CREATE SEQUENCE log_id_seq
        START WITH 1
        INCREMENT BY 1;
        
        
        --############################3
        
        CREATE OR REPLACE FUNCTION insert_category_log()
        RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO logs (id, time, table_name, values)
            VALUES (nextval('log_id_seq'), now(), 'category', row_to_json(NEW));
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        
        CREATE TRIGGER insert_category_trigger
        AFTER INSERT ON category
        FOR EACH ROW
        EXECUTE FUNCTION insert_category_log();
        
        --#################################
        
        CREATE OR REPLACE FUNCTION insert_client_log()
        RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO logs (id, time, table_name, values)
            VALUES (nextval('log_id_seq'), now(), 'client', row_to_json(NEW));
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER insert_client_trigger
        AFTER INSERT ON client
        FOR EACH ROW
        EXECUTE FUNCTION insert_client_log();
        
        --##########################
        
        CREATE OR REPLACE FUNCTION insert_payment_log()
        RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO logs (id, time, table_name, values)
            VALUES (nextval('log_id_seq'), now(), 'payment', row_to_json(NEW));
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER insert_payment_trigger
        AFTER INSERT ON payment
        FOR EACH ROW
        EXECUTE FUNCTION insert_payment_log();
        
        --###########
        
        CREATE OR REPLACE FUNCTION insert_dish_log()
        RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO logs (id, time, table_name, values)
            VALUES (nextval('log_id_seq'), now(), 'dish', row_to_json(NEW));
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER insert_dish_trigger
        AFTER INSERT ON dish
        FOR EACH ROW
        EXECUTE FUNCTION insert_dish_log();
        """
        )

task1
