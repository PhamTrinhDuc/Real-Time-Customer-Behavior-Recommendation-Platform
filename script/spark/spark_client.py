import os
from loguru import logger
from dotenv import load_dotenv
load_dotenv()
from pyspark.sql import SparkSession

current_dir = os.path.dirname(os.path.abspath(__file__))
JAR_FOLDER_PATH = os.path.join(current_dir, "jars")
JAR_PATH = ','.join([
    # minio
    f"{JAR_FOLDER_PATH}/hadoop-aws-3.3.4.jar",
    f"{JAR_FOLDER_PATH}/aws-java-sdk-bundle-1.12.262.jar", 
    f"{JAR_FOLDER_PATH}/hadoop-common-3.3.4.jar",
    # delta lake
    # f"{JAR_FOLDER_PATH}/delta-core_2.12-2.4.0.jar",
    # f"{JAR_FOLDER_PATH}/delta-storage-2.4.0.jar",
])

class SparkClient: 
    def __init__(self): 
      self.spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("SparkHiveMinIOIntegration")

        # Add JAR files
        .config("spark.jars", JAR_PATH)
        
        # Delta lake config
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # Hive Metastore Configuration
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", "thrift://localhost:9083")
        .enableHiveSupport()
        
        # MinIO (S3A) Configuration - Add compatibility configs
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        
        # Fix ClassCastException - Force file listing compatibility
        # .config("spark.sql.parquet.enableVectorizedReader", "false")
        # .config("spark.sql.adaptive.enabled", "false")
        # .config("spark.hadoop.fs.s3a.directory.marker.retention", "keep")
        
        # Hive warehouse configuration
        .config("spark.sql.warehouse.dir", "s3a://ecommerce/")
        
        # Additional Hive configuration
        .config("hive.metastore.client.factory.class", "org.apache.hadoop.hive.metastore.HiveMetaStoreClientFactory")
        .config("hive.metastore.client.socket.timeout", "300")
        
        .getOrCreate()
      )
    
    def get_spark_session(self):
        return self.spark
    
    def stop(self):
      if self.spark:
        self.spark.stop()
        print("✅ Spark session stopped")
    
    def test_delta_connection(self):
        """Test Delta Lake functionality"""
        try:
            print("=== Testing Delta Lake Connection ===")
            
            # Try to read delta table with explicit schema
            df = self.spark.read.format("delta").load("s3a://ecommerce/data_postgres/customers/")
            print("✅ Successfully read Delta table!")
            print(f"Record count: {df.count()}")
            df.show(5)
            return True
            
        except Exception as e:
            print(f"❌ Error reading Delta table: {e}")
            
            # Fallback: Try reading as parquet
            try:
                print("Trying to read as Parquet...")
                df = self.spark.read.parquet("s3a://ecommerce/data_postgres/customers/")
                print("✅ Successfully read as Parquet!")
                df.show(5)
                return True
            except Exception as e2:
                print(f"❌ Parquet also failed: {e2}")
                return False
    
    def test_hive_connection(self):
        """Test Hive Metastore connection"""
        try:
            print("=== Testing Hive Metastore Connection ===")
            
            databases = self.spark.sql("SHOW DATABASES")
            databases.show()
            
            if databases.filter(databases.databaseName == "ecommerce").count() > 0:
                tables = self.spark.sql("SHOW TABLES IN ecommerce")
                tables.show()
                return True
            else:
                print("⚠️  Database 'ecommerce' not found")
                return False
                
        except Exception as e:
            print(f"❌ Error connecting to Hive Metastore: {e}")
            return False

if __name__ == "__main__":
  client = SparkClient()
  try:
    hive_ok = client.test_hive_connection()
    delta_ok = client.test_delta_connection()
    
    if hive_ok and delta_ok:
        print("🎉 All connections working!")
    else:
        print("⚠️ Some connections failed")
  finally:
      client.stop()