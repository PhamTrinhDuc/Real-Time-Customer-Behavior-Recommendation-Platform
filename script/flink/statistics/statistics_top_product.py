import os
import sys 
from dotenv import load_dotenv
load_dotenv()

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from pyflink.datastream.functions import MapFunction, FlatMapFunction
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common.time import Time
from utils.create_stream import create_kafka_source, create_sink_kafka
from utils.sender_alert import SendAlertToWebhook
import json
import os


class ParseOrderItem(MapFunction): 
  def map(self, value): 
    try:
      order_item = json.loads(value)['payload']
      print(f"🛒 Parsed order item: {order_item}")
      
      # Kiểm tra các field cần thiết
      product_id = order_item.get('product_id')
      quantity = order_item.get('quantity')
      
      if product_id is None or quantity is None:
          print(f"❌ Missing required fields: product_id={product_id}, quantity={quantity}")
          return None
          
      return order_item
    except Exception as e:
      print(f"❌ Parse error: {e}")
      return None
    

class TopProductProcess(ProcessWindowFunction): 
  def process(self, key, context, elements): 
    from collections import defaultdict
    import json
    
    # elements: số order_item trong window 
    product_sales = defaultdict(int)
    element_count = 0

    for order_item in elements: 
      element_count += 1
      product_id = order_item.get('product_id')
      quantity = order_item.get('quantity', 0)
      
      if product_id is not None and quantity > 0:
        product_sales[product_id] += quantity
    
    if len(product_sales) == 0:
        print("⚠️ No valid products found in this window")
        return []
    
    # Sort theo số lượng giảm dần
    sorted_products = sorted(product_sales.items(), key=lambda x: x[1], reverse=True)[:5]
    window = context.window()
    
    print(f"🏆 Top 5 products: {sorted_products}")

    # Return top 5 products như một JSON string chứa array
    if sorted_products:
        results = []
        for rank, (product_id, quantity) in enumerate(sorted_products, 1):
            result = {
                'metric': 'top_product_window',
                'window_start': window.start // 1000,
                'window_end': window.end // 1000, 
                'product_id': product_id, 
                'total_quantity': quantity,
                'rank': rank,
                'window_duration_minutes': 1
            }
            results.append(result)
        
        # Return như một JSON array string thay vì list of strings
        return [json.dumps(results, ensure_ascii=False)]
    else:
        return []


class TopProductFlatMap(FlatMapFunction):
    def flat_map(self, value):
        import json
        try:
            # Parse JSON array và emit từng item
            products = json.loads(value)
            for product in products:
                yield json.dumps(product, ensure_ascii=False)
        except Exception as e:
            print(f"❌ FlatMap error: {e}")
            # Fallback: emit original value
            yield value


# main job
def top_product_job(src_topic_name: str, 
                    sink_topic_name: str,
                    group_id: str, 
                    source_name: str
                    ):
  env, raw_stream = create_kafka_source(topic=src_topic_name, 
                                        group_id=group_id, 
                                        source_name=source_name
                                        )

  order_item_stream = (raw_stream
    .map(ParseOrderItem(), output_type=Types.PICKLED_BYTE_ARRAY())
    .filter(lambda x: x is not None)
  )

  # Sử dụng Processing Time Window thay vì Event Time
  # vì order_items không có timestamp trực tiếp
  top_products = (order_item_stream
    .key_by(lambda x: "global")
    .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))  # 1 phút window cho test
    .process(TopProductProcess(), output_type=Types.LIST(Types.STRING())) 
  ) 

  result_stream = top_products.flat_map(TopProductFlatMap(), output_type=Types.STRING())
  
  # Print để debug
  # result_stream.map(lambda x: print(f"🏆 Top Product Result: {x}"), output_type=Types.STRING())

  # Sink to Kafka
  sink = create_sink_kafka(topic=sink_topic_name)
  result_stream.sink_to(sink)

  env.execute(job_name="top product statistics")

if __name__ == "__main__":
  topic = 'ecommere-cdc.public.order_items'
  sink_topic_name = "statistics_top_product"
  group_id = 'statistics-top-product-group'
  source_name = 'Kafka Source - order_items'
  top_product_job(src_topic_name=topic, 
                  sink_topic_name=sink_topic_name, 
                  group_id=group_id, 
                  source_name=source_name)