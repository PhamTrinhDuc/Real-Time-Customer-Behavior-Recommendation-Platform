
class CommonConfig: 
  DATA_SOURCE_PATH = "s3a://ecommerce/data_postgres/"
  DESTINATION_PATH = "s3a://ecommerce"

class SegmentCusConfig:
  """Configuration constants for the customer segmentation job"""
  MART_NAME = "segmentation_customer"
  TBL_LIST = ["customers", "orders"]
  
  # Customer segment thresholds (in VND)
  HIGH_VALUE_THRES = 100_000_000  # 100M VND
  MEDIUM_VALUE_THRES = 10_000_000  # 10M VND
  
  # Segment labels
  HIGH_VALUE_LABEL = "High Value"
  MEDIUM_VALUE_LABEL = "Medium Value" 
  LOW_VALUE_LABEL = "Low Value"


class ProductAnalyticConfig: 
  """Configuration constants for the analyze top 10 products job"""
  MART_NAME = "product_analytics"
  TBL_LIST = ["orders", "order_items", "products", "categories"]
  TOP_K: 10
  
