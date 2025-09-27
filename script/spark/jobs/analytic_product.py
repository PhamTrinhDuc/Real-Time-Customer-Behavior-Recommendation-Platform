"""
Analytics Product Spark Job


Author: Data Engineering Team
Created: 2025-09-26
"""
import os
import sys
from loguru import logger
from collections import defaultdict
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from spark_client import SparkClient
from config.helpers import JobConfig, ProductAnalyticConfig
from job_interface import JobInterface

class ProductAnalyticJob(JobInterface): 
  def __init__(self, spark_client: SparkClient):
    self.job_config = JobConfig(job_name=ProductAnalyticConfig.mart_name)
    super().__init__(spark_client, self.job_config)
    self.paconfig = ProductAnalyticConfig()
  
  def extract_data(self) -> dict[str, DataFrame]: 
    """
    Extract orders, order_items, categories and customers data from MinIO
    
    Returns:
      list dataframe: (orders, order_items, categories, customers)
        
    Raises:
      Exception: If data extraction fails
    """
    tbl_list = self.paconfig.tbl_list
    required_cols = self.paconfig.required_cols_dict

    results = defaultdict(DataFrame)
    for tbl in tbl_list: 
      path = os.path.join(self.job_config.source_path, tbl)
      df = self.spark.read.parquet(path)
      results[tbl] = df

      if not self._validate_dataframe(df=df, df_name=tbl, required_cols=required_cols[tbl]): 
        msg = f"Table: {tbl} failed to validate "
        self.logger.error(msg) 
        raise ValueError(msg)

    logger.info(f"Extract tables: {results.keys()} sucessful")
    return results

  def transform_data(self, tbl_extracted: dict[str, DataFrame]) -> dict[str, DataFrame]: 
    """
    Transform data into multiple business-focused tables
    Returns multiple DataFrames for different business needs
    """
    order_df = tbl_extracted["orders"].alias("o")
    order_item_df = tbl_extracted["order_items"].alias("oi")
    category_df = tbl_extracted["categories"].alias("c")
    product_df = tbl_extracted["products"].alias("p")

    try: 
      logger.info("Starting data transformation for Analyze top 10 products")
      # Base aggregation
      base_product_metrics = (
        product_df.join(category_df, F.col("p.category_id") == F.col("c.category_id"), "inner")
        .join(order_item_df, F.col("p.product_id") == F.col("oi.product_id"), "inner")
        .join(order_df, F.col("o.order_id") == F.col("oi.order_id"), "inner")
        .filter(F.col("o.status") == "completed")
        .groupBy(
            F.col("p.product_id"),
            F.col("p.name").alias("product_name"),
            F.col("c.name").alias("category_name")
        )
        .agg(
            F.sum("oi.quantity").alias("total_quantity_sold"),
            F.sum(F.col("oi.quantity") * F.col("oi.price")).alias("total_revenue"),
            F.countDistinct("o.customer_id").alias("unique_customers"),
            F.count("oi.order_item_id").alias("total_orders"),
            F.avg("oi.price").alias("avg_selling_price"),
            F.min("o.order_date").alias("first_sale_date"),
            F.max("o.order_date").alias("last_sale_date")
        )
        .withColumn("analysis_date", F.current_date())
      )

      # table 1: top product summary
      top_products_summary = (
        base_product_metrics
        .select(
          "product_id", "product_name", "category_name",
          "total_quantity_sold", "total_revenue", "unique_customers",
          "analysis_date"
        )
        .orderBy(F.desc("total_revenue"))
        .limit(self.paconfig.top_k)
      )

      # table 2: product pricing analytics
      product_pricing = (
        base_product_metrics
        .withColumn("avg_revenue_per_customer", 
                    F.col("total_revenue") / F.col("unique_customers")) # trung bình số tiền KH mua sản phẩm
        .withColumn("avg_quantity_per_customer", 
                    F.col("total_quantity_sold") / F.col("unique_customers")) # trung bình số lượng KH mua sản phẩm
        .select(
          "product_id", "product_name", "category_name", 
          "avg_selling_price", "avg_revenue_per_customer", 
          "avg_quantity_per_customer", "analysis_date"
        )
        .orderBy(F.desc("avg_revenue_per_customer"))
      )

      # table 3: product lifecycle metrics 
      product_lifecycle = (
        base_product_metrics
        .withColumn("product_lifecycle_days", 
                    F.datediff("last_sale_date", "first_sale_date")) # số ngày giữa đơn hàng đầu tiên và đơn hàng cuối cùng
        .withColumn("revenue_per_day", 
                    F.col("total_revenue") / F.col("product_lifecycle_days")) # doanh thu mỗi ngày
        .select(
          "product_id", "product_name", "category_name",
          "first_sale_date", "last_sale_date", "product_lifecycle_days", "revenue_per_day", "analysis_date"
        )
        .filter(F.col("product_lifecycle_days") > 0)
      )

      # table 4: category performance
      category_performance = (
        base_product_metrics
          .groupBy("category_name", "analysis_date")
          .agg(
            F.sum("total_revenue").alias("category_total_revenue"),
            F.sum("total_quantity_sold").alias("category_total_quantity"),
            F.countDistinct("product_id").alias("active_products_count"),
            F.avg("total_revenue").alias("avg_product_revenue_in_category")
        )
        .orderBy(F.desc("category_total_revenue"))
      )
      
      return {
        "top_products_summary": top_products_summary,
        "product_pricing_analytics": product_pricing,
        "product_lifecycle_metrics": product_lifecycle,
        "category_performance": category_performance
      }
    except Exception as e: 
      logger.error(f"Data transformation failed: {str(e)}")
      raise

  def load_to_minio(self, tbl_transformed: dict[str, DataFrame]) -> None:
    """
    Load multiple tables to MinIO with proper partitioning
    """
    try: 
      for tbl_name, df in tbl_transformed.items(): 

        output_path = os.path.join(
          self.job_config.destination_path, 
          self.paconfig.mart_name, 
          tbl_name
        )

        if tbl_name == "top_products_summary":
            partition_cols = ["analysis_date"]
        elif tbl_name == "product_pricing_analytics":
            partition_cols = ["analysis_date", "category_name"]
        elif tbl_name == "category_performance":
            partition_cols = ["analysis_date"]
        else:
            partition_cols = ["analysis_date"]
        
        self.logger.info(f"Writing {tbl_name} to {output_path}")
        (
          df.write
          .format("parquet")
          .mode("overwrite")
          .partitionBy(*partition_cols)
          .option("compression", "snappy")
          .parquet(output_path)
        )

        self.logger.info(f"✅ {tbl_name} loaded successfully. Records: {df.count()}")

    except Exception as e: 
      msg = f"Data loading failed: {str(e)}"
      self.logger.error(msg)
      raise ValueError(msg)


def main():
  """
  Main entry point for the customer segmentation job
  """
  try:
    # Initialize Spark client
    spark_client = SparkClient()
    
    # Create and run the segmentation job
    job = ProductAnalyticJob(spark_client)
    job.run()
    
    logger.info("Analytics Products Job execution completed")
      
  except Exception as e:
    logger.error(f"Job execution failed: {str(e)}")
    sys.exit(1)


if __name__ == "__main__": 
  main()