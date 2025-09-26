"""
Customer Segmentation Spark Job

This module performs customer segmentation analysis based on purchase behavior.
It reads customer and order data from MinIO, processes them to create customer segments,
and writes the results back to MinIO in partitioned parquet format.

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
from config.helpers import SegmentCusConfig, CommonConfig
from job_interface import JobInterface

class CustomerSegmentationJob(JobInterface):
  """
  Customer Segmentation Spark Job class
  
  Handles the complete workflow of customer segmentation analysis:
  1. Data extraction from MinIO
  2. Data transformation and segmentation logic
  3. Data loading back to MinIO with partitioning
  """

  def __init__(self): 
    super.__init__()

  def _validate_dataframe(self, df: DataFrame, df_name: str, required_cols: list) -> bool:
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
      if df.count() == 0: 
        logger.error(f"{df_name} dataframe is empty")
        return False

      df_cols = df.columns
      missing_cols = [col for col in required_cols if col not in df_cols]
      if missing_cols: 
        logger.error(f"{df_name} missing required columns: {missing_cols}")
        return False

      logger.info(f"{df_name} validation passed. Records: {df.count()}")
      return True
    
    except Exception as e: 
      logger.error(f"Error validating {df_name}: {str(e)}")
      return False
    
  def extract_data(self) -> tuple[DataFrame, DataFrame]: 
    """
    Extract orders and customers data from MinIO
    
    Returns:
      tuple: (orders_df, customers_df)
        
    Raises:
      Exception: If data extraction fails
    """

    order_path = os.path.join(CommonConfig.DATA_SOURCE_PATH, "orders")
    order_df = self.spark.read.parquet(order_path)
    logger.info(f"Extracted orders data from: {order_path}")

    customer_path = os.path.join(CommonConfig.DATA_SOURCE_PATH, "customers")
    customer_df = self.spark.read.parquet(customer_path)
    logger.info(f"Extracted customers data from: {customer_path}")

    cus_required_cols = ['customer_id', 'last_name', 'email']
    order_required_cols = ['customer_id', 'order_id', 'total_price']

    if not self._validate_dataframe(order_df, "orders", order_required_cols):
      raise ValueError("Orders data validation failed")
                
    if not self._validate_dataframe(customer_df, "customers", cus_required_cols):
      raise ValueError("Customers data validation failed")
    
    return order_df, customer_df
  
  def transform_data(self, order_df: DataFrame, customer_df: DataFrame): 
    """
    Transform data to create customer segmentation
    
    Args:
        orders_df: Orders dataframe
        customers_df: Customers dataframe
        
    Returns:
        DataFrame: Customer segmentation dataframe
        
    Raises:
        Exception: If data transformation fails
    """
     
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

    try: 
      logger.info("Starting data transformation for customer segmentation")
      # Join orders with customers and aggregate
      customer_metrics = (
        order_df
        .join(customer_df, on="customer_id", how="inner")
        .groupBy("customer_id", "last_name", "email")
        .agg(
          F.count("order_id").alias("total_orders"),
          F.sum("total_price").alias("total_spent"),
          F.avg("total_price").alias("avg_order_spent"),
          F.min("order_date").alias("first_order_date"),
          F.max("order_date").alias("last_order_date")
        )
      )

      # Apply customer segmentation logic 
      customer_segmentation = (
        customer_metrics
        .withColumn(
          "customer_segment", 
          F.when(
              F.col("total_spent") >= SegmentCusConfig.HIGH_VALUE_THRES, SegmentCusConfig.HIGH_VALUE_LABEL
          )
          .when(
              F.col("total_spent") >= SegmentCusConfig.MEDIUM_VALUE_THRES, 
              SegmentCusConfig.MEDIUM_VALUE_LABEL
          )
          .otherwise(SegmentCusConfig.LOW_VALUE_LABEL)
        )
        .withColumn("process_data", F.current_date())
      )

      # Add additional business metrics
      customer_segmentation = (
        customer_segmentation
        .withColumn(
          "customer_lifetime_days", 
          F.datediff(F.col("last_order_date"), F.col("first_order_date")) # số ngày giữa đơn hàng đầu tiên và đơn hàng cuối cùng
        )
        .withColumn(
          "avg_days_between_orders", 
          F.col("customer_lifetime_days") / F.col("total_orders") # trung bình mỗi đơn hàng cách nhau bao nhiêu ngày
        )
      )

      customer_segmentation = customer_segmentation.orderBy(F.desc("total_spent"))
      logger.info("Data transformation completed successfully")

      # Log segment distribution
      segment_counts = (
        customer_segmentation
        .groupBy("customer_segment")
        .count()
        .collect()
      )
      for row in segment_counts:
        logger.info(f"Segment '{row.customer_segment}': {row.count} customers")
      
      return customer_segmentation

    except Exception as e: 
      logger.error(f"Data transformation failed: {str(e)}")
      raise

  def push_data_to_minio(self, segment_df: DataFrame) -> None: 
    """
    Load segmentation data to MinIO with partitioning
    
    Args:
        segmentation_df: Customer segmentation dataframe
        
    Raises:
        Exception: If data loading fails
    """

    try: 
      logger.info("Starting data loading to MinIO")
      output_path = os.path.join(CommonConfig.DESTINATION_PATH, SegmentCusConfig.MART_NAME)
      (
         segment_df.write
         .format("parquet")
         .mode("append")
         .partitionBy("process_data", "customer_segment")
         .option("compression", "snappy")
         .parquet(output_path)
      )

      logger.info(f"Data successfully loaded to: {output_path}")
      logger.info(f"Total records written: {segment_df.count()}")
      
    except Exception as e:
      logger.error(f"Data loading failed: {str(e)}")
      raise
    
  def run(self) -> None: 
    """
    Execute the complete customer segmentation job
    
    Raises:
        Exception: If any step of the job fails
    """
    start_time = datetime.now()
    logger.info(f"Starting Customer Segmentation Job at {start_time}")
    
    try:
      # Step 1: Extract data
      orders_df, customers_df = self.extract_data()
      
      # Step 2: Transform data
      segmentation_df = self.transform_data(orders_df, customers_df)
      
      # Step 3: Load data
      self.push_data_to_minio(segmentation_df)
      
      end_time = datetime.now()
      duration = end_time - start_time
      logger.info(f"Customer Segmentation Job completed successfully in {duration}")
    except Exception as e:
        logger.error(f"Customer Segmentation Job failed: {str(e)}")
        raise
    finally:
        # Clean up Spark session
        self.spark_client.stop()

def main():
  """
  Main entry point for the customer segmentation job
  """
  try:
    # Initialize Spark client
    spark_client = SparkClient()
    
    # Create and run the segmentation job
    job = CustomerSegmentationJob(spark_client)
    job.run()
    
    logger.info("Customer Segmentation Job execution completed")
      
  except Exception as e:
    logger.error(f"Job execution failed: {str(e)}")
    sys.exit(1)

if __name__ == "__main__":
    main()