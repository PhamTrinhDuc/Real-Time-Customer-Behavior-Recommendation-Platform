from kafka import KafkaConsumer
import json
import sys
import argparse


BOOTSTRAP_SERVERS = "localhost:9092"

def check_topics(topic_name, num_messages: int=5): 
  try: 
    consumer = KafkaConsumer(
      topic_name,
      bootstrap_servers=BOOTSTRAP_SERVERS,
      auto_offset_reset='earliest',        # Đọc từ đầu nếu chưa có offset
      enable_auto_commit=False,            # Không tự commit
      group_id=f"check-{topic_name}",      # Group riêng để không ảnh hưởng consumer thật
      value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
      key_deserializer=lambda k: k.decode('utf-8') if k else None,
      consumer_timeout_ms=10000           # Timeout sau 10s nếu không có message
    )

    messages = []
    for msg in consumer:
        messages.append({
            "key": msg.key,
            "value": msg.value,
            "partition": msg.partition,
            "offset": msg.offset,
            "timestamp": msg.timestamp
        })
        if len(messages) >= num_messages:
            break

    consumer.close()

    if messages:
        print(f"✅ Tìm thấy {len(messages)} message(s) trong topic '{topic_name}':\n")
        for i, m in enumerate(messages[:1]):
            print(f"      Value: {json.dumps(m['value']['payload'], indent=2, ensure_ascii=False)}")
            print("-" * 50)
    else:
        print(f"🟡 Không tìm thấy message nào trong topic '{topic_name}' (có thể trống hoặc hết thời gian chờ).")

  except Exception as e:
      print(f"❌ Lỗi kết nối hoặc đọc topic: {e}")

  
if __name__ == "__main__": 
  parser = argparse.ArgumentParser()
  parser.add_argument(
    "-t",
    "--topic", 
    default="ecommere-cdc.public.order_items",
    help="Topic saved in Kafka",
  )
  args = parser.parse_args()
  parsed_args = vars(args)
  topic = parsed_args["topic"]

  check_topics(topic_name=topic)