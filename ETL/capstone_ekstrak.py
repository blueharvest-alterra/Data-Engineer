import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

class EkstrakData:
    def __init__(self):
        # Load environment variables from a .env file
        load_dotenv()

        # Retrieve database credentials from environment variables
        self.db_host = os.getenv("DB_HOST")
        self.db_name = os.getenv("DB_NAME")
        self.db_user = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")

        try:
            self.connection = psycopg2.connect(
                host=self.db_host,
                port=5490,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password
            )
            print("Berhasil terhubung ke database PostgreSQL!")
        except psycopg2.Error as e:
            print(f"Error saat menghubungkan ke database PostgreSQL: {e}")

    def get_table_names(self):
        table_names = []
        if self.connection:
            try:
                cursor = self.connection.cursor()
                query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema='public'
                """
                cursor.execute(query)
                table_names = [table[0] for table in cursor.fetchall()]
            except psycopg2.Error as error:
                print(f"Error saat mengambil nama tabel dari database PostgreSQL: {error}")
            finally:
                if 'cursor' in locals() and cursor:
                    cursor.close()
        return table_names

    def extract_table(self, table_name):
        df = pd.DataFrame()
        if self.connection:
            try:
                cursor = self.connection.cursor()
                query = f"SELECT * FROM {table_name}"
                cursor.execute(query)
                rows = cursor.fetchall()

                if rows:
                    column_names = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(rows, columns=column_names)
            except psycopg2.Error as error:
                print(f"Error saat mengekstrak data dari tabel {table_name}: {error}")
            finally:
                if 'cursor' in locals() and cursor:
                    cursor.close()
        return df

    def save_to_csv(self, df, table_name):
        try:
            folder_path = "/home/abdan/airflow/ekstrak"
            os.makedirs(folder_path, exist_ok=True)
            
            csv_filename = f"{table_name}.csv"
            csv_path = os.path.join(folder_path, csv_filename)
            df.to_csv(csv_path, index=False)
            print(f"Data dari tabel '{table_name}' tersimpan di '{csv_path}'")
        except Exception as e:
            print(f"Error saat menyimpan data ke CSV: {e}")

# Inisialisasi dan menjalankan kelas EkstrakData
if __name__ == "__main__":
    ekstrak_data = EkstrakData()
    table_names = ekstrak_data.get_table_names()
    for table_name in table_names:
        df = ekstrak_data.extract_table(table_name)
        if not df.empty:
            ekstrak_data.save_to_csv(df, table_name)
