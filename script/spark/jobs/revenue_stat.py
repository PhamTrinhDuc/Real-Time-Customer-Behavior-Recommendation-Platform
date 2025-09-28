import os
from collections import defaultdict
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from job_interface import JobInterface, JobConfig
from config.helpers import RevenueStatConfig
from spark_client import SparkClient

class RevenueaStatJob(JobInterface): 
  def __init__(self, spark_client: SparkClient): 
    self.job_config = JobConfig(RevenueStatConfig.mart_name)
    self.rsconfig = RevenueStatConfig()
    super().__init__(spark_client=spark_client, config=self.job_config)

  def extract_data(self) -> dict[str, DataFrame]: 
    """
    Extract orders data from MinIO
    
    Returns:
      dict dataframe: (tbl_name, df)
        
    Raises:
      Exception: If data extraction fails
    """
    tbl_list = self.rsconfig.tbl_list
    required_cols = self.rsconfig.required_cols_dict

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

  def transform_data(self, tbl_extracted):
    """
    Transform data into multiple business-focused tables
    Returns multiple DataFrames for different business needs
    """
    order_df = tbl_extracted["orders"]

    try: 
      revenue_montly = self._create_revenue_monthly(order_df=order_df)
      revenue_daily = self._create_revenue_daily(order_df=order_df)

      result_tables =  {
        "revenue_monthly": revenue_montly, 
        "revenue_daily": revenue_daily
      }
    
      self.logger.info(f"Created {result_tables.keys()} revenue analytics tables")
      return result_tables
    
    except Exception as e: 
      msg = f"Transform failed: {str(e)}"
      self.logger.info(msg)
      raise ValueError(msg)

  def _create_revenue_monthly(self, order_df: DataFrame) -> DataFrame:

    # Window for calculating period-over-period changes
    # month_window = Window.orderBy("order_month")

    revenue_monthly =  (
      order_df.filter(F.col("status") == "completed")
      .filter(F.col("order_date") >= F.add_months(start=F.current_date(), months=-12))
      .withColumn("order_month", F.date_trunc("month", F.col("order_date")))
      .groupBy(F.col("order_month"))
      .agg(
        F.countDistinct(F.col("customer_id")).alias("unique_customer"), 
        F.countDistinct(F.col("order_id")).alias("total_orders"),
        F.sum(F.col("total_price")).alias("total_revenue")
      )
      .withColumn("avg_revenue_per_customer", 
                  F.col("total_revenue") / F.col("unique_customer"))
      .withColumn("orders_per_customer", 
                  F.col("total_orders") / F.col("unique_customer"))
      
      # # === GROWTH METRICS (Month-over-Month) ===
      # .withColumn(
      #   "prev_month_revenue", F.lag("total_revenue", 1).over(month_window))

      # .withColumn(
      #   "revenue_growth_rate", 
      #   F.when(F.col("prev_month_revenue") > 0, 
      #          ((F.col("total_revenue") - F.col("prev_month_revenue")) / F.col("prev_month_revenue") * 100))
      #     .otherwise(F.lit(0))
      # )

      # .withColumn(
      #   "prev_month_customers", 
      #   F.lag("unique_customer", 1).over(month_window)
      # )

      # .withColumn(
      #   "customer_growth_rate", 
      #   F.when(F.col("prev_month_customers") > 0, 
      #          (F.col("unique_customer")-F.col("prev_month_customers")/F.col("prev_month_customers")*100))
      #   .otherwise(F.lit(0))
      # )
      .withColumn(
        "analysis_date", F.current_date()
      )
      .orderBy(F.col("order_month"))
    )
    return revenue_monthly

  def _create_revenue_daily(self, order_df: DataFrame) -> DataFrame: 

    revenue_daily = (
      order_df
      .filter(F.col("status") == "completed")
      .filter(F.col("order_date") >= F.date_sub(start=F.current_date(), days=90)) # last 90 days
      .withColumn("order_date_only", F.col("order_date").cast("date")) # convert only date
      .withColumn("day_of_week", F.dayofweek("order_date_only"))
      .withColumn("day_name", 
                  F.when(F.col("day_of_week")==1, "Sunday")
                  .when(F.col("day_of_week")==2, "Monday")
                  .when(F.col("day_of_week")==3, "Tuesday")
                  .when(F.col("day_of_week")==4, "Wednesday")
                  .when(F.col("day_of_week")==5, "Thursday")
                  .when(F.col("day_of_week")==6, "Friday")
                  .when(F.col("day_of_week")==7, "Saturday")
                  )
      .withColumn("is_weekend", 
                  F.when(F.col("day_of_week").isin([1, 7]), True).otherwise(False))
      .groupBy("order_date_only", "day_of_week", "day_name", "is_weekend")
      .agg(
          F.sum("total_price").alias("daily_revenue"),
          F.count("order_id").alias("daily_orders"),
          F.countDistinct("customer_id").alias("daily_unique_customers"),
          F.avg("total_price").alias("daily_avg_order_value")
      )
      .withColumn(
          "revenue_per_customer",
          F.col("daily_revenue") / F.col("daily_unique_customers")
      )
      .withColumn("analysis_date", F.current_date())
      .orderBy("order_date_only")
    )

    return revenue_daily

  def load_to_minio(self, tbl_transformed: dict[str, DataFrame]):
    """Load transformed revenue analytics to MinIO"""
    try:
        base_path = os.path.join(self.job_config.destination_path, self.rsconfig.mart_name)
        
        for table_name, df in tbl_transformed.items():
            output_path = os.path.join(base_path, table_name)
            
            self.logger.info(f"Loading {table_name} to {output_path}")
            
            (
              df.write
              .format("parquet")
              .mode("overwrite")
              .partitionBy("analysis_date")
              .option("compression", "snappy")
              .parquet(output_path)
            )

            self.logger.info(f"✅ {table_name} loaded successfully. Records: {df.count()}")
            
    except Exception as e:
        self.logger.error(f"Load failed: {str(e)}")
        raise

def main():
  """Main execution function"""
  try:
    spark_client = SparkClient()
    job = RevenueaStatJob(spark_client)
    success = job.run()
    
    if success:
        job.logger.info("✅ Revenue analytics job completed successfully")
    else:
        job.logger.error("❌ Revenue analytics job failed")
          
  except Exception as e:
    job.logger.error(f"💥 Job execution failed: {str(e)}")
        
if __name__ == "__main__": 
  main()