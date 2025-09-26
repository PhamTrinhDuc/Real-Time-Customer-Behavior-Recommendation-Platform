import os
import sys
from loguru import logger
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from spark_client import SparkClient


class JobInterface: 
  def __init__(self, spark_client: SparkClient, job_name: str): 
    self.spark_client = spark_client
    self.spark = self.spark_client.get_spark_session()
    logger.info(f"{job_name} initialized")
  
  def _validate_dataframe(self, df: DataFrame, df_name: str, required_cols: list) -> bool:
    pass

  def extract_data(self) -> dict[str, DataFrame]:
    pass

  def transform_data(self, tbl_extracted: dict[str, DataFrame]) -> dict[str, DataFrame]:
    pass

  def push_data_to_minio(self, tbl_transformed: dict[str, DataFrame]) -> None:
    pass
  
  def run(): 
    pass
