from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict


@dataclass
class JobConfig:
  """Base configuration for Spark jobs"""
  job_name: str
  source_path: str = "s3a://ecommerce/data_postgres/"
  destination_path: str = "s3a://ecommerce/"
  partition_columns: List[str] = None
  write_mode: str = "overwrite"
  file_format: str = "parquet"
  compression: str = "snappy"
  enable_caching: bool = True
  validation_enabled: bool = True
  
  def __post_init__(self):
    if self.partition_columns is None:
      self.partition_columns = ["process_date"]

@dataclass
class JobMetrics: 
  """Job execution metrics"""
  job_name: str 
  start_time: datetime = field(default_factory=lambda: datetime.now())
  end_time: Optional[datetime] = None
  records_processed: int = 0
  records_written: int = 0
  tables_processed: Dict[str, int] = None # [df_name, record_number]
  status: str = "RUNNING"
  error_message: Optional[str] = None

  def __post_init__(self): 
    if self.tables_processed is None: 
      self.tables_processed = {}
  
  @property
  def duration_seconds(self) -> float: 
    if self.end_time: 
      return (self.end_time - self.start_time).total_seconds()
    return 0
  
  def mask_completed(self, records_written: int=0): 
    self.end_time = datetime.now()
    self.records_written = records_written
    self.status = "SUCESS"

  def mask_failed(self, records_written: int=0): 
    self.end_time = datetime.now()
    self.records_written = records_written
    self.status = "FAILED"

@dataclass
class SegmentCusConfig:
  """Configuration constants for the customer segmentation job"""
  mart_name: str = "segmentation_customer"
  tbl_list: List[str] = field(default_factory=lambda: ["customers", "orders"])
  required_cols_dict: Dict[str, List[str]] = \
    field(default_factory=lambda: {
      "orders": ['customer_id', 'order_id', 'total_price'], 
      "customers": ['customer_id', 'last_name', 'email'],
    })
  
  # Customer segment thresholds (in VND)
  high_value_thres = 100_000_000  # 100M VND
  medium_value_thres = 10_000_000  # 10M VND
  
  # Segment labels
  high_value_label = "High Value"
  medium_value_label = "Medium Value" 
  low_value_label = "Low Value"


@dataclass
class ProductAnalyticConfig: 
  """Configuration constants for the analyze top 10 products job"""
  mart_name: str = "product_analytics"
  tbl_list: List[str] = field(default_factory=lambda: ["orders", "order_items", "products", "categories"])
  required_cols_dict: Dict[str, List[str]] = \
    field(default_factory=lambda: {
      "orders": ["order_id", "total_price", "order_date", "customer_id"], 
      "order_items": ["order_item_id", "price", "order_id", "product_id", "quantity"],
      "categories": ["category_id", "name"], 
      "products": ["product_id", "name"]
    })
  top_k: int = 10
  
