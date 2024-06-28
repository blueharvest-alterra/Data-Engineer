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
    'start_date': pendulum.datetime(2024, 6, 21, tz="Asia/Jakarta"),  # Mengatur tanggal mulai dan zona waktu
}

# Fungsi untuk mengekstrak data dari PostgreSQL
def ekstrak_data():
    ekstrak = EkstrakData()  # Membuat instance EkstrakData
    table_names = ekstrak.get_table_names()  # Mendapatkan nama-nama tabel
    for table_name in table_names:
        df = ekstrak.extract_table(table_name)  # Mengekstrak data dari setiap tabel
        if not df.empty:
            ekstrak.save_to_csv(df, table_name)  # Menyimpan data ke file CSV

# Fungsi untuk mentransformasi data
def transform_data():
    extract_table_directory = '/home/abdan/airflow/ekstrak'  # Direktori tempat file CSV disimpan
    transform = DataTransformation(extract_table_directory)  # Membuat instance DataTransformation
    files_to_upload = transform.transform_data()  # Transformasi data dan upload ke Firebase
    files_to_upload.extend(transform.merge_data())  # Menggabungkan data dan menambahkan ke daftar file yang akan diupload
    return files_to_upload

# Fungsi untuk memuat data ke BigQuery
def load_data():
    bucket_name = "micro-store-418519.appspot.com"  # Nama bucket di Firebase Storage
    dataset_id = "Capstone9"  # ID dataset di BigQuery
    credentials_path = "/home/abdan/airflow/dags/micro-store-418519-f667fac9300e.json"  # Path ke file kredensial

    loader = FirebaseToBigQueryLoader(bucket_name, dataset_id, credentials_path)  # Membuat instance FirebaseToBigQueryLoader

    # Mendapatkan tanggal hari ini dalam format YYYY-MM-DD
    today = datetime.today().strftime("%Y-%m-%d")

    # Mendaftar semua file CSV di folder untuk tanggal saat ini
    files = loader.list_files(today)

    # Memuat setiap file ke BigQuery
    if not files:
        logging.warning("No files found to load.")  # Memberikan peringatan jika tidak ada file yang ditemukan
    else:
        for file_name in files:
            table_name = os.path.splitext(os.path.basename(file_name))[0]  # Menggunakan nama file tanpa ekstensi sebagai nama tabel
            loader.load_to_bigquery(file_name, table_name)  # Memuat file ke BigQuery

# Mendefinisikan DAG
with DAG(
    dag_id="Capstone",
    default_args=default_args,
    description="DAG for project AquaQulture",
    schedule_interval="@daily"  # Menetapkan jadwal DAG untuk berjalan setiap hari
) as dag:

    # Mendefinisikan tugas menggunakan PythonOperator
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
        provide_context=True  # Memastikan Airflow memberikan execution_date
    )

    ekstrak_data_task >> transform_data_task >> load_data_task 
