import os
import sys 

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from pyflink.datastream.functions import MapFunction
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Duration
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common.time import Time
from utils.create_stream import create_kafka_source
from utils.sender_alert import SendAlertToWebhook
import json
import os

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")


# --- Parse Order JSON ---
class ParseOrder(MapFunction):
  def map(self, value):
    data = json.loads(value)
    # Chuyển order_date (giây) → mili giây cho Flink
    data["event_time"] = int(data["order_date"]) * 1000
    return data

# main job
def statistics_job():
  env, raw_stream = create_kafka_source(
    topic="orders", 
    group_id="statistics-job-group", 
    source_name="Kafka Source - Orders")
  
  # Parse JSON → Order object
  order_stream = raw_stream.map(ParseOrder(), output_type=Types.PICKLED_BYTE_ARRAY())

  # Gán timestamp & watermark
  order_stream = order_stream.assign_timestamps_and_watermarks(
    watermark_strategy=WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)) 
    .with_timestamp_assigner(lambda order, ts: order['event_time'])
  )

  # rule 1: === THỐNG KÊ 1: Doanh thu & đơn hàng mỗi phút ===
  revenue_stream = (
    order_stream.map(
      lambda order: (order.customer_id, order.total_price),
      output_type=Types.TUPLE([Types.STRING(), Types.FLOAT()])
    ) # chuyển Order → (customer_id, total_price)
    .key_by(
      lambda x: "global"
    ) # key cố định để gom tất cả về cùng một nhóm
    .window(
      window_assigner=TumblingEventTimeWindows.of(Time.minutes(1))
    ) # gom các đơn hàng trong mỗi phút riêng biệt
    .reduce(
      # acc = (2, 1000.0)  # đã có 2 đơn, tổng 1000
      # curr = ("C001", 200.0)  # đơn mới            => (3, 1200.0)
      lambda acc, curr: (acc[0] + 1, acc[1] + curr[1]), 
      # Hàm này nhận vào key, window và elements rồi phát ra một bản ghi chứa thời gian bắt đầu và kết thúc của cửa sổ cùng với số lượng đơn hàng và tổng doanh thu đã được làm tròn.
      lambda key, window, elements, out: out.collect((window.start, window.end, elements[0], round(elements[1], 2))),
      output_type=Types.TUPLE([Types.LONG(), Types.LONG(), Types.INTEGER(), Types.FLOAT()])
    )
  )

  if WEBHOOK_URL: 
    revenue_stream.map(func=SendAlertToWebhook(webhook_url=WEBHOOK_URL), 
                       output_type=Types.PICKLED_BYTE_ARRAY()).print()
  else: 
    print("⚠️ WEBHOOK_URL not set. Skipping alert sending.")
    revenue_stream.print()

  env.execute(job_name="statistic revenue in 1 minues")

