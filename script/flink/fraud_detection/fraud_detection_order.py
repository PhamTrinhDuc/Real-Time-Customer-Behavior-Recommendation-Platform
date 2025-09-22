import os
import sys 
from dotenv import load_dotenv
load_dotenv()
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Duration
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time
from pyflink.datastream.connectors.file_system import FileSink
from pyflink.common.serialization import Encoder
import json
import os
from utils.sender_alert import SendAlertToWebhook
from utils.create_stream import create_kafka_source
from utils.utils import FraudAlert, ParseOrder, OrderTimestampAssigner

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")

# Process function for frequent orders
class FrequentOrderProcessor(ProcessWindowFunction):
  def process(self, key, context, elements, out):
    orders = list(elements)
    order_count = len(orders)
    
    if order_count > 5:
      alert = FraudAlert(key, "Frequent orders", f"{order_count} orders in 1 minute")
      out.collect(alert)
  
def fraud_detection_order(topic: str, group_id: str, src_name: str): 
  env, raw_stream = create_kafka_source(
    topic=topic, 
    group_id=group_id, 
    source_name=src_name
  )

  file_sink = (
    FileSink.for_row_format("/tmp/flink_output", Encoder.simple_string_encoder())
    .build()
  )

  # Parse JSON → Dict object
  parsed_stream = (raw_stream.map(ParseOrder(), output_type=Types.PICKLED_BYTE_ARRAY())
    .filter(lambda order: order is not None)
  )

  order_stream = parsed_stream.assign_timestamps_and_watermarks(
    WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
    .with_timestamp_assigner(OrderTimestampAssigner())
  )

  # ------- Rule 1: đơn hàng > 10 triệu ----------
  high_values_alert = order_stream.filter(
     lambda order: order["total_price"] > 10000000
  ).map(
    lambda order: FraudAlert(order["customer_id"], "High value order", f"{order['total_price']:,} VND"),
    output_type=Types.PICKLED_BYTE_ARRAY()
  )

  # ======================DEBUG ========================
  # high_values_alert.map(lambda alert: str(alert), output_type=Types.STRING()).print()
  # env.execute("Test High Value Alerts")
  # return


  # -------- Rule 2: Cảnh báo nếu > 5 đơn trong 1 phút (theo customer) ----------
  frequent_order_alert = (order_stream
      .key_by(lambda order: order["customer_id"]) 
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .process(FrequentOrderProcessor(), output_type=Types.PICKLED_BYTE_ARRAY())
  )

  # Kết hợp tất cả các luồng cảnh báo
  all_alerts = high_values_alert \
    .union(frequent_order_alert) \

  # Gửi cảnh báo tới webhook
  if WEBHOOK_URL:
    all_alerts.map(SendAlertToWebhook(WEBHOOK_URL), output_type=Types.PICKLED_BYTE_ARRAY()).print()
  else:
    print("⚠️ WEBHOOK_URL not set. Skipping alert sending.")
    all_alerts.print()
  
  env.execute("Fraud Detection Order Stream")

if __name__ == "__main__": 
  topic_src = "ecommere-cdc.public.orders"
  group_id = "fraud-detection-order-group"
  src_name = "Kafka Source - Orders"
  fraud_detection_order(topic_src, group_id, src_name)