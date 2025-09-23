import os
import pandas as pd
import psycopg2
from minio import Minio
from io import BytesIO
import shutil
from pathlib import Path
from deltalake.writer import write_deltalake
from dotenv import load_dotenv
load_dotenv()

# Config
MINIO_ENDPOINT = f"localhost:{os.getenv('MINIO_PORT')}"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
LOCAL_DELTA_PATH = "./script/spark/delta_data"  # Local Delta Lake path
BUCKET_NAME = "e-commerece"
FOLDER_NAME = "data-postgres"

DB_CONFIG = {
    'host': 'localhost',
    'port': os.getenv('POSTGRES_PORT'),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD')
}

TABLES = ["orders", "order_items", "payments", "customers", "products"]

def create_minio_client():
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def create_bucket_if_not_exists(client, bucket_name):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"✅ Created bucket: {bucket_name}")
    else:
        print(f"📦 Bucket {bucket_name} already exists")

def clean_dataframe_for_delta(df, table_name):
    """Clean DataFrame để tương thích với Delta Lake"""
    print(f"🧹 Cleaning DataFrame for table {table_name}")
    
    df_cleaned = df.copy()
    
    # Xử lý các columns toàn NULL
    for col in df_cleaned.columns:
        if df_cleaned[col].isnull().all():
            print(f"⚠️ Column '{col}' is all NULL - converting to string type")
            df_cleaned[col] = df_cleaned[col].astype('object').fillna('')
        elif df_cleaned[col].dtype == 'object':
            # Đảm bảo object columns không có mixed types
            df_cleaned[col] = df_cleaned[col].astype(str).replace('nan', '')
    
    # Convert problematic data types
    for col in df_cleaned.columns:
        dtype = df_cleaned[col].dtype
        
        # Handle datetime columns
        if 'datetime' in str(dtype):
            df_cleaned[col] = pd.to_datetime(df_cleaned[col], errors='coerce')
        
        # Handle mixed int/float with nulls
        elif dtype == 'object' and col not in ['created_at', 'updated_at']:
            # Try to convert to numeric if possible
            try:
                numeric_series = pd.to_numeric(df_cleaned[col], errors='coerce')
                if not numeric_series.isnull().all():
                    df_cleaned[col] = numeric_series
            except:
                pass
    
    print(f"✅ Cleaned DataFrame shape: {df_cleaned.shape}")
    return df_cleaned

def upload_delta_table_to_minio(minio_client, local_path, bucket_name, table_name):
    """Upload toàn bộ Delta Lake table (bao gồm _delta_log) lên MinIO"""
    local_path = Path(local_path)
    
    if not local_path.exists():
        print(f"❌ Local path {local_path} không tồn tại")
        return False
        
    # Upload tất cả files trong Delta table
    for file_path in local_path.rglob('*'):
        if file_path.is_file():
            # Tạo relative path cho MinIO object
            relative_path = file_path.relative_to(local_path)
            object_name = f"{FOLDER_NAME}/{table_name}/{relative_path}"
            
            try:
                minio_client.fput_object(
                    bucket_name=bucket_name,
                    object_name=object_name,
                    file_path=str(file_path)
                )
                print(f"📤 Uploaded: {object_name}")
            except Exception as e:
                print(f"❌ Failed to upload {file_path}: {e}")
                return False
                
    return True

def pandas_to_minio():
    # Tạo local delta directory
    os.makedirs(LOCAL_DELTA_PATH, exist_ok=True)
    
    # Kết nối MinIO
    minio_client = create_minio_client()
    create_bucket_if_not_exists(minio_client, BUCKET_NAME)
    
    # Kết nối PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    
    for table in TABLES:
        print(f"🔄 Processing table: {table}")
        
        try:
            # Đọc data bằng pandas
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            
            if len(df) == 0:
                print(f"⚠️ Table {table} is empty, skipping...")
                continue
            
            # Clean DataFrame cho Delta Lake
            df_cleaned = clean_dataframe_for_delta(df, table)
            
            # Tạo Delta Lake table local
            table_path = os.path.join(LOCAL_DELTA_PATH, table)
            print(f"💾 Writing Delta Lake table to {table_path}")
            
            # Xóa table cũ nếu có
            if os.path.exists(table_path):
                shutil.rmtree(table_path)
            
            # Viết Delta Lake table với cleaned data
            write_deltalake(
                table_or_uri=table_path,
                data=df_cleaned,
                mode="overwrite",
                overwrite_schema=True
            )
            
            # Upload toàn bộ Delta table lên MinIO
            success = upload_delta_table_to_minio(
                minio_client=minio_client,
                local_path=table_path,
                bucket_name=BUCKET_NAME,
                table_name=table
            )
            
            if success:
                print(f"✅ Successfully uploaded {table} Delta table to MinIO")
            else:
                print(f"❌ Failed to upload {table}")
                
        except Exception as e:
            print(f"❌ Error processing table {table}: {str(e)}")
            continue
    
    conn.close()
    print(f"🎉 All tables processed! Local Delta files: {LOCAL_DELTA_PATH}")

if __name__ == "__main__":
    pandas_to_minio()