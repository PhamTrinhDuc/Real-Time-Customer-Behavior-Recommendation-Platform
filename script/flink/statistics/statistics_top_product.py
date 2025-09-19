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

class ParseOrderItem: 
  def __call__(self, value): 
    order_item = json.loads(value)
    order = order_item['order']
    order_item['event_time'] = int(order['order_date'])*1000
    return order_item
    

class TopProducProcess(ProcessWindowFunction): 
  def process(self, key, context, elements, out): 
    from collections import defaultdict
    # elements: số order_item trong window 1 giờ
    product_sales = defaultdict(int)

    for order_item in elements: 
      product_id = order_item['product_id']
      quantity = order_item['quantity']
      if product_id is not None and quantity > 0:
        product_sales[product_id] += quantity
    
    # Sort theo số lượng giảm dần
    sorted_product = sorted(product_sales, lambda x: x[1], reverse=True)[:5]
    window = context.window()

    for product_id, quantity in sorted_product: 
      result = {
        'metric': 'top product hourly',
        'window_st': window.start // 1000, # giây
        'window_end': window.end // 1000, 
        'product_id': product_id, 
        'quantity': quantity
      }
      out.collect(json.dumps(result, ensure_ascii=False))

# main job
def top_product_job():
  env, raw_stream = create_kafka_source(topic='order_item', 
                                        group_id='top-product-group', 
                                        source_name="Kafka Source - Order item"
                                        )

  order_item_stream = raw_stream.map(func=ParseOrderItem(), 
                                     output_type=Types.PICKLED_BYTE_ARRAY())
  
  order_item_stream = order_item_stream.assign_timestamps_and_watermarks(
    watermark_strategy=WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)) 
    .with_timestamp_assigner(lambda order_item, ts: order_item['event_time'])
  )

  top_products = (order_item_stream
    .key_by(lambda x: "global")
    .window(TumblingEventTimeWindows.of(Time.hours(1)))
    .process(TopProducProcess(), output_type=Types.STRING) 
  ) 

  if WEBHOOK_URL: 
    top_products.map(func=SendAlertToWebhook(webhook_url=WEBHOOK_URL), 
                       output_type=Types.PICKLED_BYTE_ARRAY()).print()
  else: 
    print("⚠️ WEBHOOK_URL not set. Skipping alert sending.")
    top_products.print()

  env.execute(job_name="top product in 1 hours")




