import json
from dotenv import dotenv_values

env = dotenv_values(".env")  # Trả về dict: {'DB_USER': '...', 'DB_PASSWORD': '...'}

config = {
  "name": "ecommere-sink",
  "config": {
      "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
      "tasks.max": "1",
      "topics": "",
      "connection.port": "",
      "connection.dbname": "",
      "connection.user": "",
      "connection.password": "",
      "auto.create": True,
      "consumer.auto.offset.reset": "latest"
  }
}
config["config"]["topics"] = env.get("KAFKA_TOPIC", "ecommere")
config["config"]["connection.user"] = env.get("POSTGRES_USER", "")
config["config"]["connection.password"] = env.get("POSTGRES_PASSWORD", "")
config['config']['connection.port'] = env.get("POSTGRES_PORT", "")
config['config']['connection.dbname'] = env.get("POSTGRES_DB", "")


with open("./config/kafka-connect/connect-db-sink.json", "w") as f:
    json.dump(config, f, indent=4)
