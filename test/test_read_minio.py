import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from script.spark.spark_client import SparkClient
from pandas import DataFrame

client = SparkClient().spark

bucket_name = "ecommerce"
prefixs = {
    "product_analytics": ["category_performance", "product_lifecycle_metrics", "product_pricing_analytics", "top_products_summary"],
    "revenue_statistics": ["revene_monthly", "revenue_daily"],
    "segmentation_customer": []
}

for prefix in prefixs.keys():
    if len(prefixs[prefix]) == 0:
      # Đọc trực tiếp thư mục gốc
      path = f"s3a://{bucket_name}/{prefix}"
      try:
          df = client.read.parquet(path)
          pandas_df: DataFrame = df.toPandas()
          print(f"Data from {prefix}:")
          print(pandas_df.head())
          print(50 * "=")
      except Exception as e:
          print(f"Error reading {path}: {e}")
    else: 
      # Đọc từng thư mục con riêng biệt
      for sub_folder in prefixs[prefix]: 
        path_sub = f"s3a://{bucket_name}/{prefix}/{sub_folder}" 
        try:
            df = client.read.parquet(path_sub)
            pandas_df: DataFrame = df.toPandas()
            print(f"Data from {prefix}/{sub_folder}:")
            print(pandas_df.info())
            print(50 * "=")
        except Exception as e:
            print(f"Error reading {path_sub}: {e}")