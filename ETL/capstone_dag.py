import os
import pendulum
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from capstone_ekstrak import EkstrakData
from capstone_transform import DataTransformation
from capstone_load import FirebaseToBigQueryLoader 

default_args = {
    'owner': 'Capstone Kelompok 9',
    'retries': 3,
    'start_date': pendulum.datetime(2024, 6, 21, tz="Asia/Jakarta"),
}

def ekstrak_data():
    ekstrak = EkstrakData()
    table_names = ekstrak.get_table_names()
    for table_name in table_names:
        df = ekstrak.extract_table(table_name)
        if not df.empty:
            ekstrak.save_to_csv(df, table_name)

def transform_data():
    extract_table_directory = '/home/abdan/airflow/ekstrak'
    transform = DataTransformation(extract_table_directory)
    files_to_upload = transform.transform_data()
    files_to_upload.extend(transform.merge_data())
    return files_to_upload

def load_data(**kwargs):
    bucket_name = "micro-store-418519.appspot.com"
    dataset_id = "Capstone9"
    credentials_path = "/home/abdan/airflow/dags/micro-store-418519-f667fac9300e.json"  # Update with your service account JSON path

    loader = FirebaseToBigQueryLoader(bucket_name, dataset_id, credentials_path)

    # Get current date in YYYY-MM-DD format
    today = datetime.today().strftime("%Y-%m-%d")

    # List all CSV files in the folder for the current date
    files = loader.list_files(today)

    # Load each file into BigQuery
    if not files:
        logging.warning("No files found to load.")
    else:
        for file_name in files:
            table_name = os.path.splitext(os.path.basename(file_name))[0]  # Use file name without extension as table name
            loader.load_to_bigquery(file_name, table_name)


# Define the DAG
with DAG(
    dag_id="Capstone",
    default_args=default_args,
    description="DAG for project AquaQulture",
    schedule_interval="@daily"
) as dag:

    # Define tasks using PythonOperator
    ekstrak_data_task = PythonOperator(
        task_id="Ekstrak",
        python_callable=ekstrak_data
    )

    transform_data_task = PythonOperator(
        task_id="Transform",
        python_callable=transform_data
    )

    load_data_task = PythonOperator(
        task_id="Load",
        python_callable=load_data,
        provide_context=True  # Ensure Airflow passes execution_date
    )

    # Define task dependencies
    ekstrak_data_task >> transform_data_task >> load_data_task
