from google.cloud import bigquery, storage  
from datetime import datetime  
import os 
import logging  

class FirebaseToBigQueryLoader:
    def __init__(self, bucket_name, dataset_id, credentials_path):
        self.bucket_name = bucket_name  
        self.dataset_id = dataset_id 
        self.client = bigquery.Client.from_service_account_json(credentials_path)  # Membuat klien BigQuery dari file kredensial
        self.storage_client = storage.Client.from_service_account_json(credentials_path) 
        logging.basicConfig(level=logging.INFO) 

    def list_files(self, today):
        """Daftar semua file di folder yang ditentukan dalam bucket."""
        try:
            prefix = f"{today}/"  # Menentukan prefix folder berdasarkan tanggal hari ini
            blobs = self.storage_client.list_blobs(self.bucket_name, prefix=prefix)  # Mendaftar semua blob di folder
            files = [blob.name for blob in blobs if blob.name.lower().endswith('.csv')]  # Menyaring file dengan ekstensi .csv
            logging.info(f"Found {len(files)} files in {self.bucket_name}/{prefix}")
            logging.debug(f"Files found: {files}")
            return files 

        except Exception as e:
            logging.error(f"Error listing files: {str(e)}")
            return []  # Mengembalikan daftar kosong jika terjadi kesalahan

    def load_to_bigquery(self, file_name, table_name):
        try:
            dataset_ref = self.client.dataset(self.dataset_id)  # Referensi dataset di BigQuery
            table_ref = dataset_ref.table(table_name)  # Referensi tabel di BigQuery

            job_config = bigquery.LoadJobConfig(
                autodetect=True,  # Mengaktifkan autodetect skema
                source_format=bigquery.SourceFormat.CSV,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Truncate tabel sebelum memuat data baru
            )

            uri = f"gs://{self.bucket_name}/{file_name}"  

            load_job = self.client.load_table_from_uri(
                uri, table_ref, job_config=job_config
            ) 

            logging.info(f"Starting job {load_job.job_id} to load {uri} into {self.dataset_id}.{table_name}")

            load_job.result()  

            logging.info(f"Job {load_job.job_id} completed successfully.")

        except Exception as e:
            logging.error(f"Error loading {uri} into BigQuery: {str(e)}")

if __name__ == "__main__":
    bucket_name = "micro-store-418519.appspot.com"  # Nama bucket di Google Cloud Storage
    dataset_id = "Capstone9"  # ID dataset di BigQuery
    credentials_path = "/home/abdan/airflow/dags/micro-store-418519-f667fac9300e.json"  # Path ke file kredensial

    loader = FirebaseToBigQueryLoader(bucket_name, dataset_id, credentials_path)  # Membuat instance dari kelas FirebaseToBigQueryLoader

    # Mendapatkan tanggal hari ini dalam format YYYY-MM-DD
    today = datetime.today().strftime("%Y-%m-%d")

    # Mendaftar semua file CSV di folder untuk tanggal saat ini
    files = loader.list_files(today)

    # Memuat setiap file ke BigQuery
    if not files:
        logging.warning("No files found to load.")
    else:
        for file_name in files:
            table_name = os.path.splitext(os.path.basename(file_name))[0]  # Mengambil nama tabel dari nama file
            loader.load_to_bigquery(file_name, table_name)  # Memuat file ke BigQuery
