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
from collections import defaultdict
from datetime import datetime
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from spark_client import SparkClient
from config.helpers import SegmentCusConfig, JobConfig
from job_interface import JobInterface

class CustomerSegmentationJob(JobInterface):
  """
  Customer Segmentation Spark Job class
  
  Handles the complete workflow of customer segmentation analysis:
  1. Data extraction from MinIO
  2. Data transformation and segmentation logic
  3. Data loading back to MinIO with partitioning
  """

  def __init__(self, spark_client: SparkClient): 
    self.job_config = JobConfig(job_name=SegmentCusConfig.mart_name)
    super().__init__(spark_client=spark_client, config=self.job_config)
    self.csjob = SegmentCusConfig()

  def extract_data(self) -> dict[str, DataFrame]: 
    """
    Extract orders and customers data from MinIO
    
    Returns:
      dict: (str, DataFrame)
        
    Raises:
      Exception: If data extraction fails
    """

    tbl_list = self.csjob.tbl_list
    required_cols = self.csjob.required_cols_dict

    results = defaultdict(DataFrame)
    for tbl in tbl_list: 
      path = os.path.join(self.job_config.source_path, tbl)
      df = self.spark.read.parquet(path)
      results[tbl] = df

      if not self._validate_dataframe(df=df, df_name=tbl, required_cols=required_cols[tbl]): 
        msg = f"Table: {tbl} failed to validate "
        self.logger.error(msg) 
        raise ValueError(msg)

    self.logger.info(f"Extract tables: {results.keys()} sucessful")
    return results
  
  def transform_data(self, df_extracted: dict[str, DataFrame]): 
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


    order_df = df_extracted['orders']
    customer_df = df_extracted['customers']

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
              F.col("total_spent") >= self.csjob.high_value_thres, self.csjob.high_value_label
          )
          .when(
              F.col("total_spent") >= self.csjob.medium_value_thres, 
              self.csjob.medium_value_label
          )
          .otherwise(self.csjob.low_value_label)
        )
        .withColumn("process_date", F.current_date())
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
      self.logger.info("Data transformation completed successfully")

      # Log segment distribution
      segment_counts = (
        customer_segmentation
        .groupBy("customer_segment")
        .count()
        .collect()
      )
      for row in segment_counts:
        self.logger.info(f"Segment '{row.customer_segment}': {row.count} customers")
      
      return customer_segmentation

    except Exception as e: 
      self.logger.error(f"Data transformation failed: {str(e)}")
      raise

  def load_to_minio(self, segment_df: DataFrame) -> None: 
    """
    Load segmentation data to MinIO with partitioning
    
    Args:
        segmentation_df: Customer segmentation dataframe
        
    Raises:
        Exception: If data loading fails
    """

    try: 
      self.logger.info("Starting data loading to MinIO")
      output_path = os.path.join(self.job_config.destination_path, self.csjob.mart_name)
      (
         segment_df.write
         .format("parquet")
         .mode("append")
         .partitionBy("process_data", "customer_segment")
         .option("compression", "snappy")
         .parquet(output_path)
      )

      self.logger.info(f"Data successfully loaded to: {output_path}")
      self.logger.info(f"Total records written: {segment_df.count()}")
      
    except Exception as e:
      self.logger.error(f"Data loading failed: {str(e)}")
      raise
    
  def run(self) -> None: 
    """
    Execute the complete customer segmentation job
    
    Raises:
        Exception: If any step of the job fails
    """
    start_time = datetime.now()
    self.logger.info(f"Starting Customer Segmentation Job at {start_time}")
    
    try:
      # Step 1: Extract data
      df_extracted = self.extract_data()
      
      # Step 2: Transform data
      segmentation_df = self.transform_data(df_extracted=df_extracted)
      
      # Step 3: Load data
      self.load_to_minio(segmentation_df)
      
      end_time = datetime.now()
      duration = end_time - start_time
      self.logger.info(f"Customer Segmentation Job completed successfully in {duration}")
    except Exception as e:
        self.logger.error(f"Customer Segmentation Job failed: {str(e)}")
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