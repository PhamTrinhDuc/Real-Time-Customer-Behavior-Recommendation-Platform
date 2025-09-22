from pyflink.datastream.functions import MapFunction
from pyflink.common.watermark_strategy import TimestampAssigner
import json
import os
from dateutil import parser

class OrderTimestampAssigner(TimestampAssigner):
  def extract_timestamp(self, element, record_timestamp):
    return element["timestamp"]
  
# --- Parse Order JSON ---
class ParseOrder(MapFunction):
  def map(self, value):
      try:
          # dữ liệu thô từ Kafka là chuỗi JSON, dữ liệu order trong payload
          data = json.loads(value)['payload']
          
          order_date_str = data.get("order_date")
          if not order_date_str:
              print("❌ Missing order_date")
              return None

          # Chuyển ISO string → datetime → timestamp (giây) → milliseconds
          dt = parser.parse(order_date_str)
          timestamp_ms = int(dt.timestamp() * 1000)

          # Thêm trường timestamp để Flink dùng
          data["timestamp"] = timestamp_ms

          return data

      except Exception as e:
          print(f"❌ Parse error: {e}, raw value: {value[:200]}...")
          return None

class FraudAlert:
  def __init__(self, customer_id, reason, value=None):
    self.customer_id = customer_id
    self.reason = reason
    self.value = value

  def __str__(self):
    extra = f" (Value: {self.value})" if self.value else ""
    return f"🚨 Fraud Alert: Customer {self.customer_id} | {self.reason}{extra}"