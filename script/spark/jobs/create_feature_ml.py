import os
import sys
from loguru import logger
from collections import defaultdict
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from spark_client import SparkClient
from config.helpers import JobConfig, FeatureMLCfg
from job_interface import JobInterface

class FeatureML(JobInterface): 
  def __init__(self, spark_client: SparkClient):
    self.job_config = JobConfig(job_name=FeatureMLCfg.mart_name)
    super().__init__(spark_client, self.job_config)
    self.fm_config = FeatureMLCfg()

  def extract_data(self) -> dict[str, DataFrame]: 
    """
    Extract orders, order_items, wishlist, cart and customers data from MinIO
    
    Returns:
      list dataframe: (orders, order_items, wishlist, cart, customers)
        
    Raises:
      Exception: If data extraction fails
    """
    tbl_list = self.fm_config.tbl_list
    required_cols = self.fm_config.required_cols_dict

    results = defaultdict(DataFrame)
    for tbl in tbl_list: 
      path = os.path.join(self.job_config.source_path, tbl)
      df = self.spark.read.parquet(path)
      results[tbl] = df

      if required_cols: 
        if not self._validate_dataframe(df=df, df_name=tbl, required_cols=required_cols[tbl]): 
          msg = f"Table: {tbl} failed to validate "
          self.logger.error(msg) 
          raise ValueError(msg)

    self.logger.info(f"Extract tables: {results.keys()} sucessful")
    return results
  
  def _create_weight(self, df: DataFrame): 
    # Calculate purchase weight with quantity and recency
    current_date = F.curdate()
    df_weighted = (
      df
      .withColumn("days_ago", F.date_diff(current_date, F.col("timestamp")))
      .withColumn("recency_weight", F.when(F.col("days_ago") <= 30, 1.0)
                                      .when(F.col("days_ago") <= 90, 0.8)
                                      .when(F.col("days_ago") <= 180, 0.6)
                                      .otherwise(0.4))
      .withColumn("weight", F.col("quantity") * F.col("recency_weight") * F.lit(1.0))
      .select(
        "customer_id", "product_id", "weight", "timestamp"
      )
    )
    return df_weighted
  
  def _create_user_item_interaction(self, 
                                    order_df: DataFrame, 
                                    order_items_df: DataFrame, 
                                    customer_df: DataFrame, 
                                    cart_df: DataFrame,
                                    wishlist_df: DataFrame) -> DataFrame:
    # Purchase interactions with quantity and recency
    purchase_inter = (
      order_df.alias("o")
        .join(order_items_df.alias("oi"), F.col("o.order_id") == F.col("oi.order_id")) 
        .select(
          F.col("o.customer_id").alias("customer_id"), 
          F.col("oi.product_id").alias("product_id"), 
          F.col("oi.quantity").cast("double").alias("quantity"),
          F.col("o.order_date").alias("timestamp"),
          F.lit("purchase").alias("inter_type")
        ).distinct()
    ) 
    purchase_weighted = self._create_weight(df=purchase_inter)

    wishlist_inter = (
      customer_df.alias("c")
      .join(wishlist_df.alias("w"), F.col("c.customer_id") == F.col("w.customer_id"))
      .select(
        F.col("w.product_id").alias("product_id"),
        F.col("c.customer_id").alias("customer_id"), 
        F.lit(0.6).alias("weight"), 
        F.col("w.added_at").alias("timestamp"),
        F.lit("wishlist").alias("inter_type")
      ).distinct()
    )

    cart_inter = (
      customer_df.alias("c")
      .join(cart_df.alias("ca"), F.col("c.customer_id") == F.col("ca.customer_id"))
      .select(
        F.col("c.customer_id").alias("customer_id"), 
        F.col("ca.product_id").alias("product_id"),
        F.col("ca.quantity").cast("double").alias("quantity"),
        F.col("ca.added_at").alias("timestamp")
      ).distinct()
    )
    cart_weighted = self._create_weight(df=cart_inter)

    # Combine all interactions
    user_item_inter = (
        purchase_weighted.select("customer_id", "product_id", "weight", "timestamp")
        .union(wishlist_inter.select("customer_id", "product_id", "weight", "timestamp"))
        .union(cart_weighted.select("customer_id", "product_id", "weight", "timestamp"))
    ).groupBy("customer_id", "product_id") \
     .agg(
         F.avg("weight").alias("avg_weight"),
         F.max("timestamp").alias("last_interaction"),
         F.count("*").alias("interaction_count")
     ) \
     .orderBy(F.desc("avg_weight"))

    user_item_inter.show(5)

    return user_item_inter

  def _create_user_features(self, 
                            customer_df: DataFrame,
                              user_item_inter: DataFrame, 
                              order_df: DataFrame) -> DataFrame:
    """Create comprehensive user features for recommendation"""
    
    # Basic interaction stats
    user_stats = user_item_inter.groupBy("customer_id").agg(
        F.sum("avg_weight").alias("total_activity_score"),
        F.count("product_id").alias("unique_products_interacted"),
        F.avg("avg_weight").alias("avg_interaction_strength"),
        F.max("last_interaction").alias("last_activity_date"),
        F.sum("interaction_count").alias("total_interactions")
    )
    
    # Purchase behavior features
    purchase_stats = (
        order_df.groupBy("customer_id").agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_spent"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("order_date").alias("first_purchase_date"),
            F.max("order_date").alias("last_purchase_date")
        ).withColumn(
            "customer_lifetime_days", 
            F.datediff(F.col("last_purchase_date"), F.col("first_purchase_date"))
        ).withColumn(
            "purchase_frequency", 
            F.col("total_orders") / (F.col("customer_lifetime_days") + 1)
        )
    )
    
    # Customer demographics + behavior
    user_features = (
        customer_df.select("customer_id", "age", "gender", "location", "registration_date")
        .join(user_stats, "customer_id", "left")
        .join(purchase_stats, "customer_id", "left")
        .fillna(0)
    )
    
    return user_features

  def _create_product_features(self, 
                               user_item_inter: DataFrame, 
                               product_df: DataFrame, 
                               order_items_df: DataFrame) -> DataFrame:
    """Create comprehensive product features"""
    
    # Popularity metrics
    product_popularity = user_item_inter.groupBy("product_id").agg(
        F.sum("avg_weight").alias("total_popularity_score"),
        F.count("customer_id").alias("total_customers_interacted"),
        F.avg("avg_weight").alias("avg_user_rating"),
        F.max("last_interaction").alias("last_interaction_date")
    )
    
    # Sales performance
    product_sales = order_items_df.groupBy("product_id").agg(
        F.sum("quantity").alias("total_sold"),
        F.count("order_id").alias("total_orders"),
        F.avg("price").alias("avg_selling_price"),
        F.sum(F.col("quantity") * F.col("price")).alias("total_revenue")
    )
    
    # Product attributes + metrics
    product_features = (
        product_df.select("product_id", "category_id", "brand", "price", "created_at", "rating", "review_count")
        .join(product_popularity, "product_id", "left")
        .join(product_sales, "product_id", "left")
        .withColumn("days_since_launch", F.datediff(F.current_date(), F.col("created_at")))
        .fillna(0)
    )
    
    return product_features
  
  def transform_data(self, tbl_extracted: dict[str, DataFrame]):
    order_df = tbl_extracted['orders']
    order_item_df = tbl_extracted['order_items']
    wishlist_df = tbl_extracted['wishlists']
    customer_df = tbl_extracted['customers']
    cart_df = tbl_extracted['carts']
    product_df = tbl_extracted['products']  # Cần thêm vào extract

    # Core interaction matrix
    user_item_inter = self._create_user_item_interaction(
        order_df=order_df, 
        order_items_df=order_item_df, 
        customer_df=customer_df, 
        cart_df=cart_df,
        wishlist_df=wishlist_df
    )
    
    # Additional feature tables
    user_features = self._create_user_features(customer_df, user_item_inter, order_df)
    product_features = self._create_product_features(user_item_inter, product_df, order_item_df)
    
    # Return dictionary of all feature tables
    return {
      'user_item_interactions': user_item_inter,
      'user_features': user_features,
      'product_features': product_features,
    }

  def load_to_minio(self, tbl_transformed):
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

        self.logger.info(f"Writing {tbl_name} to {output_path}")
        (
          df.write
          .format("parquet")
          .mode("overwrite")
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
    job = FeatureML(spark_client=spark_client)
    tbl_extracted = job.extract_data()
    job.transform_data(tbl_extracted=tbl_extracted)
      
  except Exception as e:
    logger.error(f"Job execution failed: {str(e)}")
    sys.exit(1)

if __name__ == "__main__": 
  main()