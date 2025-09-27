import os
import sys
from loguru import logger
from datetime import datetime
from abc import ABC, abstractmethod
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from pyspark.sql import DataFrame
from spark_client import SparkClient
from config.helpers import JobConfig, JobMetrics

class ValidationError(Exception):
  """Custom exception for data validation errors"""
  pass

class JobInterface(ABC): 
  """
  Abstract base class for all Spark ETL jobs
  
  Provides a standardized framework for:
  - Configuration management
  - Data extraction, transformation, and loading
  - Data validation and quality checks
  - Error handling and logging
  - Performance monitoring
  - Resource cleanup
  """
   
  def __init__(self, spark_client: SparkClient, config: JobConfig): 
    self.config = config
    self.spark_client = spark_client
    self.spark = self.spark_client.get_spark_session()
    self.metrics = JobMetrics(
      job_name=config.job_name,
      start_time=datetime.now()
    )
    self.logger = logger
    self.logger.add(
      f"logs/{config.job_name}/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log",
      mode="a",
      format="{time} | {level} | {message}",
      rotation="5 MB",
      retention="7 days"
    )
    self.logger.info(f"{config.job_name} initialized")
  
  def _validate_dataframe(self, df: DataFrame, df_name: str, required_cols: list = None) -> bool:
    """
      Comprehensive dataframe validation
      
      Args:
          df: DataFrame to validate
          df_name: Name for logging purposes
          required_columns: List of required column names
          
      Returns:
          bool: True if validation passes
          
      Raises:
          ValidationError: If validation fails and validation_enabled=True
    """
    try: 
      validation_errors = []

      # check df exists
      if df is None: 
        validation_errors.append(f"{df_name} is None")
      
      # check if df is empty
      if df.count() == 0: 
        validation_errors.append(f"{df_name} is Empty")
      
      df_cols = df.columns
      missing_cols = [col for col in required_cols if col not in df_cols]
      if missing_cols: 
        validation_errors.append(f"{df_name} missing columns: {missing_cols}")
      
      record_count = df.count()
      self.metrics.tables_processed[df_name] = record_count
      self.logger.info(f"Table: {df_name} validation passed: {record_count:,} records")
      
      if validation_errors: 
        error_msg = f"Validation failed for {df_name}: {'|'.join(validation_errors)}"
        self.logger.error(error_msg)

        if self.config.validation_enabled: 
          raise ValidationError(error_msg)
        return False
      
    except Exception as e:
          error_msg = f"Error during validation of {df_name}: {str(e)}"
          self.logger.error(error_msg)
          if self.config.validation_enabled:
              raise ValidationError(error_msg)
          return False
    
    return True

  @abstractmethod
  def extract_data(self) -> dict[str, DataFrame]:
    """
    Extract data from source systems
    
    Returns:
        Dict[str, DataFrame]: Dictionary of extracted DataFrames
        
    Raises:
        NotImplementedError: Must be implemented by subclasses
    """
    pass
  
  @abstractmethod
  def transform_data(self, tbl_extracted: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """
    Transform extracted data according to business logic
    
    Args:
        extracted_tables: Dictionary of extracted DataFrames
        
    Returns:
        Dict[str, DataFrame]: Dictionary of transformed DataFrames
        
    Raises:
        NotImplementedError: Must be implemented by subclasses
    """
    pass

  @abstractmethod
  def load_to_minio(self, tbl_transformed: dict[str, DataFrame]) -> None:
    """
    Load transformed data to destination
    
    Args:
        transformed_tables: Dictionary of transformed DataFrames
        
    Raises:
        NotImplementedError: Must be implemented by subclasses
    """
    pass
  
  def run(self): 
    """
    Execute the complete ETL job workflow
    
    Returns:
        bool: True if job completed successfully
    """
    try:
        self.logger.info(f"🎯 Starting {self.config.job_name}")
      
        # Step 1: Extract
        self.logger.info("Step 1: Extracting data...")
        extracted_tables = self.extract_data()
        self.metrics.records_processed = sum(
            df.count() for df in extracted_tables.values()
        )

        # Step 2: Transform  
        self.logger.info("Step 2: Transforming data...")
        transformed_tables = self.transform_data(extracted_tables)
        
        # Step 3: Load
        self.logger.info("Step 3: Loading data...")
        self.load_to_minio(transformed_tables)
        
        # Mark success
        self.metrics.mask_completed(self.metrics.records_written)
        self.logger.info(f"{self.config.job_name} completed successfully!")
        
        return True
    
    except Exception as e:
      # Mark failure
      self.metrics.mask_failed(str(e))
      self.logger.error(f"{self.config.job_name} failed: {str(e)}")
      return False
        
    finally:
      self.spark_client.stop()
