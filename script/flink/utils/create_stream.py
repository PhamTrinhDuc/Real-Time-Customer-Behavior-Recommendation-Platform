from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Duration
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time
import os
import json

JARS_PATH = f"{os.getcwd()}/config/kafka-connect/jars/"
BOOTSTRAP_SERVERS = "localhost:9092"

def create_kafka_source(topic: str, group_id: str, source_name: str): 
  try: 
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism=2)

    # Thêm các file JAR cần thiết để kết nối với Kafka
    # Flink cần các dependency này để giao tiếp với Kafka cluster
    jar_files = [
      f"file://{JARS_PATH}/flink-connector-kafka-1.17.1.jar",
      f"file://{JARS_PATH}/kafka-clients-3.4.0.jar",
      f"file://{JARS_PATH}/flink-shaded-guava-30.1.1-jre-15.0.jar",
    ]
    env.add_jars(*jar_files)

    # kafka source
    kafka_source = (
    KafkaSource.builder() 
      .set_bootstrap_servers(BOOTSTRAP_SERVERS) # Địa chỉ Kafka broker
      .set_topics(topic)          # Tên topic để đọc dữ liệu
      .set_group_id(group_id) # Consumer group ID
      # .set_starting_offsets(KafkaOffsetsInitializer.latest())  # Đọc từ message mới nhất
      .set_starting_offsets(KafkaOffsetsInitializer.latest())  # Đọc từ message mới nhất
      .set_value_only_deserializer(SimpleStringSchema())    # Deserializer cho dữ liệu
      .build()
    )

    raw_stream = env.from_source(
      source=kafka_source, 
      watermark_strategy=WatermarkStrategy.no_watermarks(), 
      source_name=source_name
    )

    print("Kafka source created successfully.")

    return env, raw_stream
  except Exception as e: 
    print("Error creating Kafka source:", e)
    raise e


def create_sink_kafka(): 
  pass