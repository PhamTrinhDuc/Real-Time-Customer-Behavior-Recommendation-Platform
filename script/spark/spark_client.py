import os
from pyspark.sql import SparkSession

current_dir = os.path.dirname(os.path.abspath(__file__))
JAR_FOLDER_PATH = os.path.join(current_dir, "jars")
JAR_PATH = ','.join([
    f"{JAR_FOLDER_PATH}/hadoop-aws-3.3.4.jar",
    f"{JAR_FOLDER_PATH}/aws-java-sdk-bundle-1.12.262.jar", 
    f"{JAR_FOLDER_PATH}/hadoop-common-3.3.4.jar"
])


class SparkClient: 
  def __init__(self): 
    self.spark = (
      SparkSession.builder
      .master("local[*]")
      .appName("SparkHiveMinIOIntegration")
      
      # Hive Metastore Configuration
      .config("spark.sql.catalogImplementation", "hive")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()

      # Add JAR files
      .config("spark.jars", JAR_PATH)
      
      # MinIO (S3A) Configuration!
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "project-de-access-key")
      .config("spark.hadoop.fs.s3a.secret.key", "project-de-secret-key")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
      .config("spark.hadoop.fs.s3a.fast.upload", "true")
      
      # Hive warehouse configuration
      .config("spark.sql.warehouse.dir", "s3a://ecommerce/")
      
      # # Additional Hive configuration
      .config("hive.metastore.client.factory.class", "org.apache.hadoop.hive.metastore.HiveMetaStoreClientFactory")
      .config("hive.metastore.client.socket.timeout", "300")
      
      .getOrCreate()
    )