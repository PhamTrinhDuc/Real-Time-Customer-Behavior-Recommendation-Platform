import json
from dotenv import dotenv_values

env = dotenv_values(".env")  # Trả về dict: {'DB_USER': '...', 'DB_PASSWORD': '...'}

config = {
  "name": "ecommere-cdc", 
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgresql",
    "database.port": None,
    "database.user": None,
    "database.password": None,
    "database.dbname" : None,
    "plugin.name": "pgoutput",
    "database.server.name": "test",
    "table.include.list": None
  }
}

config["config"]["database.user"] = env.get("POSTGRES_USER", "")
config["config"]["database.password"] = env.get("POSTGRES_PASSWORD", "")
config['config']['database.port'] = env.get("POSTGRES_PORT", "")
config['config']['database.dbname'] = env.get("POSTGRES_DB", "") 

TABLE_NEEDED_CDC = ['customers', 'products', 'orders', 'order_items', 'categories', 'payment']
SERVER_NAME = "ecommere-cdc"
TABLE_NEEDED_CDC = ["public." + item for item in TABLE_NEEDED_CDC]
config['config']['table.include.list'] = ",".join(TABLE_NEEDED_CDC)
config['config']['database.server.name'] = SERVER_NAME

with open("./config/cdc-connect/postgresql-cdc.json", "w") as f:
    json.dump(config, f, indent=4)