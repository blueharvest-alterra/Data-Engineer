import os  
import psycopg2 
import pandas as pd  
from dotenv import load_dotenv  

class EkstrakData:
    def __init__(self):
        # Memuat variabel lingkungan dari file .env
        load_dotenv()
        
        # Mengambil informasi koneksi database dari variabel lingkungan
        self.db_host = os.getenv("DB_HOST")
        self.db_name = os.getenv("DB_NAME")
        self.db_user = os.getenv("DB_USER")
        self.db_password = os.getenv("DB_PASSWORD")

        # Mencoba untuk terhubung ke database PostgreSQL
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
        table_names = []  # Membuat list kosong untuk menyimpan nama tabel
        if self.connection:
            try:
                cursor = self.connection.cursor()  # Membuat cursor untuk eksekusi query
                query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema='public'
                """  # Query untuk mengambil nama tabel dari schema 'public'
                cursor.execute(query)  # Mengeksekusi query
                table_names = [table[0] for table in cursor.fetchall()]  # Menyimpan hasil query ke list
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
            folder_path = "/home/abdan/airflow/ekstrak"  # Menentukan path folder untuk menyimpan CSV
            os.makedirs(folder_path, exist_ok=True)  # Membuat folder jika belum ada
            
            csv_filename = f"{table_name}.csv"  # Menentukan nama file CSV
            csv_path = os.path.join(folder_path, csv_filename) 
            df.to_csv(csv_path, index=False)  
            print(f"Data dari tabel '{table_name}' tersimpan di '{csv_path}'")
        except Exception as e:
            print(f"Error saat menyimpan data ke CSV: {e}")

# Inisialisasi dan menjalankan kelas EkstrakData
if __name__ == "__main__":
    ekstrak_data = EkstrakData() 
    table_names = ekstrak_data.get_table_names()  # Mendapatkan nama-nama tabel dari database
    for table_name in table_names:  # Iterasi melalui setiap nama tabel
        df = ekstrak_data.extract_table(table_name)  
        if not df.empty: 
            ekstrak_data.save_to_csv(df, table_name) 
