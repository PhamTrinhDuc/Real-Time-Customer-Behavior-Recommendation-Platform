import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from script.spark.spark_client import SparkClient

def main(): 
    client = SparkClient()
    
    # Test connection to Hive Metastore
    print("\n=== Testing Hive Metastore Connection ===")
    try:
        databases = client.spark.sql("SHOW DATABASES")
        print("Available databases:")
        databases.show()
        
        tables = client.spark.sql("SHOW TABLES IN ecommerce")
        print("Tables in ecommerce database:")
        tables.show()
    except Exception as e:
        print(f"❌ Error connecting to Hive Metastore: {e}")
    
    # Test MinIO connection
    print("\n=== Testing MinIO Connection ===")
    try:
        # Test reading from MinIO bucket
        df = client.spark.read.parquet("s3a://ecommerce/data_postgres/customers")
        print("✅ Successfully connected to MinIO!")
        print("Sample data from customers:")
        df.show(5)
    except Exception as e:
        print(f"❌ Error connecting to MinIO: {e}")

    client.spark.stop()
    print("\n✅ Spark session stopped successfully!")

if __name__ == "__main__": 
    main()