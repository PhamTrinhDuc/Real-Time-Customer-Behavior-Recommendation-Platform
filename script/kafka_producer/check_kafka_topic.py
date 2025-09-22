from kafka import KafkaConsumer
import json
import sys
import argparse


BOOTSTRAP_SERVERS = "localhost:9092"

def safe_json_deserialize(data):
    """Safely deserialize JSON data, return raw string if failed"""
    if data is None:
        return None
    try:
        decoded = data.decode('utf-8')
        return json.loads(decoded)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        # For debugging broken data
        decoded = data.decode('utf-8', errors='replace')
        return {"error": str(e), "raw_data": decoded}

def check_topics(topic_name, num_messages: int=5): 
  try: 
    consumer = KafkaConsumer(
      topic_name,
      bootstrap_servers=BOOTSTRAP_SERVERS,
      auto_offset_reset='earliest',        # Đọc từ đầu nếu chưa có offset
      enable_auto_commit=False,            # Không tự commit
      group_id=f"check-{topic_name}",      # Group riêng để không ảnh hưởng consumer thật
      value_deserializer=safe_json_deserialize,
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
        for i, m in enumerate(messages[:3]):  # Show first 3 messages
            print(f"Message {i+1}:")
            print(f"  Key: {m['key']}")
            print(f"  Partition: {m['partition']}, Offset: {m['offset']}")
            
            # Handle different value formats
            if isinstance(m['value'], dict):
                if 'payload' in m['value']:
                    print(f"  Payload: {json.dumps(m['value']['payload'], indent=2, ensure_ascii=False)}")
                elif 'error' in m['value']:
                    print(f"  Error: {m['value']['error']}")
                    print(f"  Raw Data: {repr(m['value']['raw_data'])}")  # Use repr to show escape chars
                else:
                    print(f"  Value: {json.dumps(m['value'], indent=2, ensure_ascii=False)}")
            else:
                print(f"  Value: {m['value']}")
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