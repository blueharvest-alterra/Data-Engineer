import pandas as pd 
import os 
import firebase_admin
from firebase_admin import credentials, storage 
from datetime import datetime 

class DataTransformation:
    def __init__(self, extract_table_directory):
        self.extract_table_directory = extract_table_directory  # Menyimpan direktori ekstrak tabel
        self.initialize_firebase()  # Inisialisasi koneksi Firebase

    def initialize_firebase(self):
        cred = credentials.Certificate("/home/abdan/airflow/dags/micro-store-418519-firebase-adminsdk-3693d-740141e01a.json")
        firebase_admin.initialize_app(cred, {
            'storageBucket': 'micro-store-418519.appspot.com' 
        })
        self.bucket = storage.bucket() 

    def check_quality_data(self, df, table_name):
        # Memeriksa kualitas data dan mencetak informasi terkait
        print(f"Data Quality Check for table {table_name}:")
        print("Missing values:")
        print(df.isnull().sum())
        print("Duplicated rows:")
        print(df.duplicated().sum())
        print("Data types:")
        print(df.dtypes)
        print("---------------------------------------------")

    def transform_data(self):
        files_to_upload = []  # Membuat list untuk menyimpan file yang akan diunggah

        # Iterasi melalui semua file di direktori ekstrak tabel
        for file in os.listdir(self.extract_table_directory):
            file_path = os.path.join(self.extract_table_directory, file)
            if os.path.isfile(file_path) and file.endswith('.csv'):
                table_name = file.split('.')[0]  # Mengambil nama tabel dari nama file
                try:
                    df = pd.read_csv(file_path)
                    
                    # Memeriksa kualitas data sebelum transformasi
                    self.check_quality_data(df, table_name)

                    # Melakukan transformasi spesifik untuk setiap tabel
                    if table_name == 'products':
                        df.rename(columns={'name': 'product_name'}, inplace=True)
                    
                    elif table_name == 'customers':
                        df['birth_date'] = pd.to_datetime(df['birth_date'], format= '%Y-%m-%d')
                    
                    elif table_name == 'transactions':
                        df['customer_id'] = df['customer_id'].astype(str)

                    elif table_name == 'transaction_details':
                        df['transaction_id'] = df['transaction_id'].astype(str)
                        df['id'] = df['id'].astype(str)
                        df['product_id'] = df['product_id'].astype(str)

                    # Mengunggah langsung ke Firebase tanpa menyimpan lokal
                    temp_file_path = f"/tmp/{table_name}.csv"
                    df.to_csv(temp_file_path, index=False)
                    self.upload_to_firebase(temp_file_path, table_name) 
                    files_to_upload.append(temp_file_path)

                except Exception as e:
                    print(f"Error processing table {table_name}: {str(e)}")

        return files_to_upload 

    def merge_data(self):
        files_to_upload = []  # Membuat list untuk menyimpan file yang akan diunggah

        try:
            # Membaca data dari file CSV
            transactions = pd.read_csv(os.path.join(self.extract_table_directory, 'transactions.csv'))
            transaction_details = pd.read_csv(os.path.join(self.extract_table_directory, 'transaction_details.csv'))
            products = pd.read_csv(os.path.join(self.extract_table_directory, 'products.csv'))
            customer_addresses = pd.read_csv(os.path.join(self.extract_table_directory, 'customer_addresses.csv'))

            # Menggabungkan customers dengan customer_addresses
            cust_address = pd.merge(customer_addresses, transactions, left_on='customer_id', right_on='customer_id', how='inner')
            
            # Menggabungkan transactions dengan cust_address yang dihasilkan
            transactiondetail = pd.merge(transaction_details, cust_address, left_on='transaction_id', right_on='id', how='inner')
            
            # Menggabungkan products dengan transactiondetail yang dihasilkan
            final_df = pd.merge(products, transactiondetail, left_on='id', right_on='product_id', how='inner')

            # Membuat tabel fakta untuk transaksi produk
            fact_products_transactions = final_df[['customer_id', 'address_id', 'product_id', 'transaction_id', 'payment_id', 'courier_id', 'promo_id', 'total']]
            fact_products_transactions.rename(columns={'total': 'total_amount'}, inplace=True)

            # Menyimpan DataFrame yang dihasilkan ke file CSV (opsional)
            fact_table_name = 'fact_products_transactions.csv'
            fact_table_path = os.path.join(self.extract_table_directory, fact_table_name)
            fact_products_transactions.to_csv(fact_table_path, index=False)
            print(f"Saved transformed data as {fact_table_name} in {self.extract_table_directory}")
            print("---------------------------------------------")

            files_to_upload.append(fact_table_path)

        except Exception as e:
            print(f"Error merging data: {str(e)}")

        return files_to_upload  

    def upload_to_firebase(self, file_path, table_name):
        try:
            today = datetime.today().strftime('%Y-%m-%d')  # Mendapatkan tanggal hari ini
            file_name = f"{table_name}.csv"
            blob = self.bucket.blob(f'{today}/{file_name}')  
            blob.upload_from_filename(file_path) 
            print(f"Uploaded {file_name} to Firebase at {today}/{file_name}")
        
        except Exception as e:
            print(f"Error uploading {file_name} to Firebase: {str(e)}")

if __name__ == "__main__":
    extract_table_directory = "/home/abdan/airflow/ekstrak"  # Menentukan direktori ekstrak tabel
    transform = DataTransformation(extract_table_directory)  # Membuat instance dari kelas DataTransformation
    
    # Transformasi dan unggah data
    files_to_upload = transform.transform_data()

    # Gabungkan data dan unggah tabel fakta
    files_to_upload.extend(transform.merge_data())

    for file_path in files_to_upload:
        transform.upload_to_firebase(file_path, os.path.basename(file_path))
