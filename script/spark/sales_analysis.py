from pyspark.sql import functions as F
from spark_client import SparkClient
import os

DATA_MINIO_PATH = "s3a://ecommerce/data_postgres/"

def main(): 
    client = SparkClient()
    spark = client.spark

    orders = spark.read.parquet(os.path.join(DATA_MINIO_PATH, "orders"))
    customers = spark.read.parquet(os.path.join(DATA_MINIO_PATH, "customers"))

    ## 1. Spark SQL Approach
    # orders.createOrReplaceTempView("orders")
    # customers.createOrReplaceTempView("customers")

    # customer_seg = spark.sql ("""
    #   SELECT
    #     c.customer_id, c.last_name, c.email, 
    #     COUNT(o.order_id) as total_orders,
    #     SUM(o.total_price) as total_spent,
    #     AVG(o.total_price) as avg_spent,
    #     CASE
    #       WHEN SUM(o.total_price) > 100000000 THEN 'high value'
    #       WHEN SUM(o.total_price) > 10000000 THEN 'medium value'
    #       ELSE 'low value'
    #     END as customer_segment
    #   FROM orders o 
    #   LEFT JOIN customers c ON o.customer_id = c.customer_id
    #   GROUP BY c.customer_id, c.last_name, c.email
    #   ORDER BY total_spent DESC
    # """)

    # 2. Spark Datafram Approach
    customer_seg = orders.join(customers, on="customer_id", how="left") \
      .groupBy("customer_id", "last_name", "email") \
      .agg(
         F.count("order_id").alias("total orders"), 
         F.sum("total_price").alias("total spent"),
         F.avg("total_price").alias("avg order spent")
      ) \
      .withColumn(
          "customer_segment", 
          F.when(F.col("total spent") > 100000000, "High Value")
            .when(F.col("total spent") > 1000000, "Medium Value")
            .otherwise("Low Value")
      ) \
      .orderBy(F.desc("total spent"))
    
    print(customer_seg.show(5))
        
if __name__ == "__main__": 
    main()