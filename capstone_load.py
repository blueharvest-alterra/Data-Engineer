from google.cloud import bigquery, storage
from datetime import datetime
import os
import logging

class FirebaseToBigQueryLoader:
    def __init__(self, bucket_name, dataset_id, credentials_path):
        self.bucket_name = bucket_name
        self.dataset_id = dataset_id
        self.client = bigquery.Client.from_service_account_json(credentials_path)
        self.storage_client = storage.Client.from_service_account_json(credentials_path)
        logging.basicConfig(level=logging.INFO)

    def list_files(self, today):
        """List all files in the specified folder in the bucket."""
        try:
            prefix = f"{today}/"
            blobs = self.storage_client.list_blobs(self.bucket_name, prefix=prefix)
            files = [blob.name for blob in blobs if blob.name.lower().endswith('.csv')]
            logging.info(f"Found {len(files)} files in {self.bucket_name}/{prefix}")
            logging.debug(f"Files found: {files}")
            return files

        except Exception as e:
            logging.error(f"Error listing files: {str(e)}")
            return []

    def load_to_bigquery(self, file_name, table_name):
        try:
            dataset_ref = self.client.dataset(self.dataset_id)
            table_ref = dataset_ref.table(table_name)

            job_config = bigquery.LoadJobConfig(
                autodetect=True,  # Enable schema autodetection
                source_format=bigquery.SourceFormat.CSV,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Truncate table before loading new data
            )

            uri = f"gs://{self.bucket_name}/{file_name}"

            load_job = self.client.load_table_from_uri(
                uri, table_ref, job_config=job_config
            )

            logging.info(f"Starting job {load_job.job_id} to load {uri} into {self.dataset_id}.{table_name}")

            load_job.result()  # Wait for the job to complete

            logging.info(f"Job {load_job.job_id} completed successfully.")

        except Exception as e:
            logging.error(f"Error loading {uri} into BigQuery: {str(e)}")

if __name__ == "__main__":
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
