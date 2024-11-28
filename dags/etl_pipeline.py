# etl_pipeline.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from airflow.hooks.base_hook import BaseHook

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'csv_to_postgres_etl',
    default_args=default_args,
    description='ETL pipeline that extracts data from a CSV file, transforms it, and loads it into PostgreSQL.',
    schedule_interval=timedelta(days=1),
)

# Function to extract data from CSV
def extract(**kwargs):
    file_path = kwargs['dag_run'].conf.get('csv_file_path', None)
    if file_path is None:
        raise ValueError("CSV file path not provided in DAG run configuration")
    df = pd.read_csv(file_path)
    return df.to_dict()

# Function to transform data
def transform(**kwargs):
    ti = kwargs['ti']
    df_dict = ti.xcom_pull(task_ids='extract')
    df = pd.DataFrame.from_dict(df_dict)

    # Example transformation: Add a new column
    df['new_column'] = df['existing_column'] * 2
    return df.to_dict()

# Function to load data into PostgreSQL
def load(**kwargs):
    ti = kwargs['ti']
    df_dict = ti.xcom_pull(task_ids='transform')
    df = pd.DataFrame.from_dict(df_dict)

    # PostgreSQL connection details
    conn = BaseHook.get_connection('postgres_default')
    connection = psycopg2.connect(
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port
    )
    cursor = connection.cursor()

    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS my_table (
            column1 INT,
            column2 VARCHAR(255),
            new_column INT
        )
    """)
    connection.commit()

    # Insert data into the table
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO my_table (column1, column2, new_column) VALUES (%s, %s, %s)
        """, (row['column1'], row['column2'], row['new_column']))
    connection.commit()
    cursor.close()
    connection.close()

# Define tasks
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task
