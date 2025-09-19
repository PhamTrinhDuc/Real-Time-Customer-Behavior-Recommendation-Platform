from pyflink.datastream.functions import MapFunction
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Duration
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time
import json
import os
from backend.models.order import Order
from utils.sender_alert import SendAlertToWebhook
from utils.create_stream import create_kafka_source


WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")

# --- Parse Order JSON ---
class ParseOrder(MapFunction):
  def map(self, value):
    data = json.loads(value)
    # Chuyển order_date (giây) → mili giây cho Flink
    data["event_time"] = int(data["order_date"]) * 1000
    return data

class FraudAlert:
  def __init__(self, customer_id, reason, value=None):
    self.customer_id = customer_id
    self.reason = reason
    self.value = value

  def __str__(self):
    extra = f" (Value: {self.value})" if self.value else ""
    return f"🚨 Fraud Alert: Customer {self.customer_id} | {self.reason}{extra}"
  
def fraud_detection_order(): 
  env, raw_stream = create_kafka_source(
    topic="orders", 
    group_id="fraud-detection-order-group", 
    source_name="Kafka Source - Orders")

  # Parse JSON → Order object
  order_stream = raw_stream.map(ParseOrder(), output_type=Types.PICKLED_BYTE_ARRAY())
  # Gán timestamp & watermark
  order_stream = order_stream.assign_timestamps_and_watermarks(
    watermark_strategy=WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)), 
    timestamp_assigner=lambda order, ts: order.timestamp  # Sử dụng timestamp từ dữ liệu
  )

  # ------- Rule 1: đơn hàng > 10 triệu ----------
  high_values_alert = order_stream.filter(
     lambda order: order.total_price > 10000000
  ).map(
    lambda order: FraudAlert(order.customer_id, "High value order", f"{order.total_price:,} VND"),
    output_type=Types.PICKLED_BYTE_ARRAY()
  )

  # ------- Rule 2: cảnh báo nếu số lượng > 20 ----------
  large_quantity_alert = order_stream.filter(
     lambda order: order.quantity > 20 
  ).map(
    lambda order: FraudAlert(order.customer_id, "Large quantity order", f"{order.quantity} items"),
    output_type=Types.PICKLED_BYTE_ARRAY()
  )

  # -------- Rule 3: Cảnh báo nếu > 5 đơn trong 1 phút (theo customer) ----------
  frequent_order_alert = order_stream.key_by(
     lambda order: order.customer_id, 
  ).windown(TumblingEventTimeWindows.of(Time.minutes(1))) \
  .reduce(
     # Gộp số lượng đơn hàng trong cửa sổ 
     lambda o1, o2: Order(
        order_id=o1.order_id, 
        customer_id=o1.customer_id, 
        total_price=o1.total_price + o2.total_price, 
        quantity=o1.quantity + o2.quantity, 
        timestamp=max(o1.timestamp, o2.timestamp)
     ), 
     lambda key, window, agg_order, out : ( 
        out.collect(FraudAlert(key, "Frequent orders", f"{agg_order.quantity} orders in 1 minute"))
        if agg_order.quantity > 5 else None
     )
  )

  # Kết hợp tất cả các luồng cảnh báo
  all_alerts = high_values_alert.union(large_quantity_alert, frequent_order_alert)

  # Gửi cảnh báo tới webhook
  if WEBHOOK_URL:
    all_alerts.map(SendAlertToWebhook(WEBHOOK_URL), output_type=Types.PICKLED_BYTE_ARRAY()).print()  # In ra console để kiểm tra
  else:
    print("⚠️ WEBHOOK_URL not set. Skipping alert sending.")
    all_alerts.print()
  
  env.execute("Fraud Detection Order Stream")


    