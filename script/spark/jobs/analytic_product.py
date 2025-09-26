"""
Analysis Top 10 Product Spark Job



Author: Data Engineering Team
Created: 2025-09-26
"""
import os
import sys
from loguru import logger
from datetime import datetime
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from spark_client import SparkClient
from config.helpers import CommonConfig, ProductAnalyticConfig 
from job_interface import JobInterface

class ProductAnalyticJob(JobInterface): 
  def __init__(self):
    super.__init__()
  
  def _validate_dataframe(self, 
                          dataframe: DataFrame, 
                          df_name: str, 
                          required_cols: list) -> bool: 
    """
    Validate dataframe structure and data quality
    
    Args:
        df: DataFrame to validate
        df_name: Name of the dataframe for logging
        required_columns: List of required column names
        
    Returns:
        bool: True if validation passes, False otherwise
    """
    try: 
      if dataframe.count() == 0: 
        logger.error(f"{df_name} dataframe is empty")
        return False

      df_cols = dataframe.columns
      missing_cols = [col for col in required_cols if col not in df_cols]
      if missing_cols: 
        logger.error(f"{df_name} missing required columns: {missing_cols}")
        return False

      logger.info(f"{df_name} validation passed. Records: {dataframe.count()}")
      return True
    
    except Exception as e: 
      logger.error(f"Error validating {df_name}: {str(e)}")
      return False
    
  def extract_data(self) -> dict[str, DataFrame]: 
    """
    Extract orders, order_items, categories and customers data from MinIO
    
    Returns:
      list dataframe: (orders, order_items, categories, customers)
        
    Raises:
      Exception: If data extraction fails
    """
    tbl_list = ProductAnalyticConfig.TBL_LIST

    results = {}
    for tbl in tbl_list: 
      path = os.path.join(CommonConfig.DATA_SOURCE_PATH, tbl)
      df = self.spark.read.parquet(path)
      results[tbl] = df
    logger.info(f"Extract tables: {results.keys()} sucessful")
    return results

  def transform_data(self, tbl_extracted: dict[str, DataFrame]) -> dict[str, DataFrame]: 
    """
    Transform data into multiple business-focused tables
    Returns multiple DataFrames for different business needs
    """
    order_df = tbl_extracted['orders'].alias("o")
    order_item_df = tbl_extracted['order_items'].alias("oi")
    category_df = tbl_extracted['categories'].alias("c")
    product_df = tbl_extracted['products'].alias("p")

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
        .limit(ProductAnalyticConfig.TOP_K)
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

  def push_data_to_minio(self, tbl_transformed: dict[str, DataFrame]) -> None:
    """
    Load multiple tables to MinIO with proper partitioning
    """
    try: 
      for tbl_name, df in tbl_transformed.items(): 
        output_path = os.path.join(
          CommonConfig.DESTINATION_PATH, 
          ProductAnalyticConfig.MART_NAME, 
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
        
        logger.info(f"Writing {tbl_name} to {output_path}")
        (
          df.write
          .format("parquet")
          .mode("overwrite")
          .partitionBy(*partition_cols)
          .option("compression", "snappy")
          .parquet(output_path)
        )

        logger.info(f"✅ {tbl_name} loaded successfully. Records: {df.count()}")

    except Exception as e: 
      logger.error(f"Data loading failed: {str(e)}")
      raise

  def run(self): 
    """
    Execute the complete analytics products job
    
    Raises:
        Exception: If any step of the job fails
    """
    start_time = datetime.now()
    logger.info(f"Starting Analytics Products Job at {start_time}")
    
    try:
      # Step 1: Extract data
      tbl_extracted = self.extract_data()

      # Step 2: Transform data 
      tbl_transformed = self.transform_data(tbl_extracted=tbl_extracted)

      # Step 3: Load data to minio
      self.push_data_to_minio(tbl_transformed=tbl_transformed)

      end_time = datetime.now()
      duration = end_time - start_time
      logger.info(f"Analytics Products Job completed successfully in {duration}")
    except Exception as e:
        logger.error(f"Analytics Products Job failed: {str(e)}")
        raise
    finally:
        # Clean up Spark session
        self.spark_client.stop()