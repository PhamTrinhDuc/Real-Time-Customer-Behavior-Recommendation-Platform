import os
import sys 
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyflink.datastream.functions import MapFunction, ReduceFunction, ProcessWindowFunction
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Duration
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
from pyflink.datastream.connectors.file_system import FileSink
from pyflink.common.serialization import Encoder
from utils.create_stream import create_kafka_source, create_sink_kafka
from utils.utils import ParseOrder, OrderTimestampAssigner
import json
import os

# Custom ReduceFunction for revenue calculation
class RevenueReduceFunction(ReduceFunction):
    def reduce(self, value1, value2):
        # value1 = (count, total_amount)
        # value2 = (customer_id, amount) from new order
        if isinstance(value1, tuple) and len(value1) == 2:
            # value1 is already accumulated (count, total)
            return (value1[0] + 1, value1[1] + value2[1])
        else:
            # First accumulation: value1 = (customer_id, amount), value2 = (customer_id, amount)
            return (2, value1[1] + value2[1])

# ProcessWindowFunction to add window information
class RevenueWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        # elements contains the reduced result: (count, total_amount)
        element_list = list(elements)
        if not element_list:
            print("⚠️  No elements in window!")
            return
            
        for element in element_list:
            count = element[0]
            total_amount = round(element[1], 2)
            window_start = context.window().start
            window_end = context.window().end
            
            yield (window_start, window_end, count, total_amount)

# main job
def statistics_job(src_topic_name: str, 
                   sink_topic_name: str, 
                   group_id: str, 
                   source_name: str
                  ):
  env, raw_stream = create_kafka_source(
    topic=src_topic_name, 
    group_id=group_id, 
    source_name=source_name
  )

  # Parse JSON → Order object
  parsed_stream = (raw_stream.map(ParseOrder(), output_type=Types.PICKLED_BYTE_ARRAY())
    .filter(lambda order: order is not None)
  )

  # Gán timestamp & watermark
  order_stream = parsed_stream.assign_timestamps_and_watermarks(
    watermark_strategy=WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)) 
    .with_timestamp_assigner(OrderTimestampAssigner())
  )
  
  ## DEBUG ##
  # order_stream.map(lambda x: print(f"Incoming order: {x}"), output_type=Types.STRING())
  # env.execute("statistic revenue in 1 minutes")
  # return

  # rule 1: === THỐNG KÊ 1: Doanh thu & đơn hàng mỗi phút ===
  revenue_stream = (
    order_stream.map(
      lambda order: (order['customer_id'], order['total_price']),
      output_type=Types.TUPLE([Types.INT(), Types.DOUBLE()])
    ) # chuyển Order → (customer_id, total_price)
    .key_by(
      lambda x: "global"
    ) # key cố định để gom tất cả về cùng một nhóm
    .window(
      window_assigner=TumblingProcessingTimeWindows.of(Time.seconds(10))  # Changed to Processing Time
    ) # gom các đơn hàng trong mỗi 10 giây (processing time)
    .reduce(
      reduce_function=RevenueReduceFunction(),
      window_function=RevenueWindowFunction(),
      output_type=Types.TUPLE([Types.LONG(), Types.LONG(), Types.INT(), Types.FLOAT()])
    )
  )

  # tạo stream riêng cho print (debug)
  debug_stream = revenue_stream.map(lambda x: str(f"📊 Revenue in last 1 minute: {x}"),
                      output_type=Types.STRING())
  debug_stream.print()
  
  sink = create_sink_kafka(topic=sink_topic_name)

  revenue_stream.map(lambda x: json.dumps({
      'metric': 'revenue_window',
      'window_start': x[0] // 1000, # giây
      'window_end': x[1] // 1000, 
      'total_orders': x[2],
      'total_revenue': x[3],
      'window_duration_minutes': 1
    }, ensure_ascii=False), output_type=Types.STRING()
  ).sink_to(sink)
  
  env.execute(job_name="statistic revenue in 1 minutes")

if __name__ == "__main__": 
  topic_name = "ecommere-cdc.public.orders"
  sink_topic_name = "statistics_revenue"
  group_id = "statistics-revenue-group"
  source_name = "Kafka Source - Orders"
  statistics_job(topic_name, sink_topic_name, group_id, source_name)