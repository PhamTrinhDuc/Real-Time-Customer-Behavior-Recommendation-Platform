# test_kafka_read.py
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.watermark_strategy import WatermarkStrategy
import time
import os

env = StreamExecutionEnvironment.get_execution_environment()
JARS_PATH = f"{os.getcwd()}/config/kafka-connect/jars/"

jar_files = [
  f"file://{JARS_PATH}/flink-connector-kafka-1.17.1.jar",
  f"file://{JARS_PATH}/kafka-clients-3.4.0.jar",
  f"file://{JARS_PATH}/flink-shaded-guava-30.1.1-jre-15.0.jar",
]
env.add_jars(*jar_files)

# Dùng tên topic chính xác
topic = "ecommere-cdc.public.orders"

# Tạm dùng latest để xem tất cả
offset_config = KafkaOffsetsInitializer.latest()

source = KafkaSource.builder() \
    .set_bootstrap_servers("localhost:9092") \
    .set_topics(topic) \
    .set_group_id(f"debug-{int(time.time())}") \
    .set_starting_offsets(offset_config) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

stream = env.from_source(source, watermark_strategy=WatermarkStrategy.no_watermarks(), source_name="Test Source")
stream.map(lambda x: print(f"📩 Received: {x}"))  # Có log rõ ràng

env.execute("Test Kafka Connection")