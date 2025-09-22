# READL TIME CUSTOMER BEHAVIOR RECOMMENDATION PLATFORM

## TABLE OF CONTENTS: 
1. [Generate database](#1-Generate-dataset)
2. [Configuration Capture Data Changes](#2-Configuration-capture-data-changes)
3. [Flink jobs](#3-flink-jobs)

## 1. Generate dataset
### 1.1 Prerequisites
- Setup environment : 
```bash
conda create -n <env_name> python=3.9 
``` 
- Activate environment: 
```bash
conda activate <env_name>
```
- Install libraries
```bash
pip install -r backend/requirements.txt
```
- Run services use docker compose: 
```bash 
docker compose -f docker/docker-compose-kafka.yml up -d
```

### 1.2 Database Schema

The database includes the following entities based on the ERD:

- **Customer**: User account information
- **Product**: Product catalog with categories
- **Category**: Product categories
- **Order**: Customer orders with items
- **OrderItem**: Individual items in orders
- **Cart**: Shopping cart functionality
- **Wishlist**: Customer wishlist
- **Payment**: Payment processing
- **Shipment**: Order shipping information

### 1.3 Entity Diagram
![alt text](images/database-ERD.png)
>See more details about how to design the database: https://vertabelo.com/blog/er-diagram-for-online-shop/
### 1.3 Database Management
- I use the database with Docker, you can configure the database in the file: `/backend/.env` depending on your database.
- Or you can run the `postgres` service in the `docker/docker-compose-kafka.yaml` file
```bash
# Initialize Table Database
python backend/utils/init_db.py --init
# Reset Database (Drop and recreate tables)
python backend/utils/init_db.py --reset
```

### 1.5 Generate database
- Here, I will create fixed data for the tables Categories, Customers, Products: 
```bash
# Create 15 categories, 200 products, 100 customers
python backend/generate_data.py --categories 15 --products 200 --customers 100
# or use the default setting
python backend/generate_data.py
# Delete all data
python backend/generate_data.py --clear
```
### 1.6 User behavior simulation
- I will simulate user behavior to create data for the remaining tables.
- Specific actions:
  - **Browsing**: View products, browse categories
  - **Cart Management**: Add/remove products from the cart
  - **Wishlist**: Add favorite products
  - **Order Placement**: Place orders from the cart
  - **Payment**: Process payment (credit card, paypal, etc.)
  - **Shipping**: Create shipping information
- Weights for each action:
  ```python
    action_weights = {
        'view_product': 0.4,      # 40% view products
        'add_to_cart': 0.2,       # 20% add to cart
        'add_to_wishlist': 0.15,  # 15% add to wishlist
        'remove_from_cart': 0.1,  # 10% remove from cart
        'place_order': 0.1,       # 10% order
        'browse_category': 0.05   # 5% browse category
    }
    ```
- Follow code below: 
```bash
# Simulate 50 shopping session, each session 25 minutes
python simulate_user_behavior.py --sessions 50 --duration 25
# Create history data 30 days
python simulate_user_behavior.py --historical 30
# Delete all data
python simulate_user_behavior.py --clear
```
### 1.7 Check the data through the API: 
- Run fastapi app and access to `http://localhost:8000/docs`: 
```bash
python backend/main.py
```
#### Basic Endpoints
- `GET /` - Root endpoint with API information
- `GET /health` - Health check
- `GET /docs` - Interactive API documentation
#### Data Endpoints
- `GET /customers` - List all customers
- `GET /products` - List all products  
- `GET /categories` - List all categories
- `GET /carts` - List all carts
- `GET /orders` - List all orders
- `GET /order_item` - List all order_item
- `GET /wishlist` - List all wishlist
- `GET /payment` - List all payment

## 2. Configuration Capture data changes
### 2.1 Connect Debezium with PostgreSQL to receive any updates from the database
```shell
bash config/cdc-connect/run.sh register_connector config/cdc-connect/postgresql-cdc.json
```
- You should see the output similar to the below example
```shell
Registering a new connector from configs/postgresql-cdc.json
HTTP/1.1 201 Created
Date: Sat, 13 Sep 2025 06:59:18 GMT
Location: http://localhost:8083/connectors/ecommere-cdc
Content-Type: application/json
Content-Length: 524
Server: Jetty(9.4.44.v20210927)

{"name":"ecommere-cdc","config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.hostname":"postgresql","database.port":"5432","database.user":"project_de_user","database.password":"project_de_password","database.dbname":"project_de_db","plugin.name":"pgoutput","database.server.name":"ecommere-cdc","table.include.list":"public.customers,public.products,public.order,public.order_items,public.categories","name":"ecommere-cdc"},"tasks":[],"type":"source"}
```
- To delete connector:
```shell
bash config/cdc-connect/run.sh delete_connector ecommere-cdc
```
> Conntector status `failed` on Debezium-ui. Access into `http://localhost:8083/connectors/[connector_name]/status` to view logs

- Access to debezium ui through link `http://localhost:8080`: 
![debezium-ui](images/debezium-ui.png)
### 2.2 Test capture change data
- Run code insert, update, delete data in database
```bash
python test/test_cdc.py
```
- Access control-center-ui via link `http://localhost:9021`, Topic -> ecommere-cdc.public.customers -> messages: 
- Or you can run the code snippet below to see the messages: 
```bash
python script/kafka_producer/check_kafka_topic.py
# you can run with the assigned topic to view messages in topic
python script/kafka_producer/check_kafka_topic.py --topic ecommere-cdc.public.orders
```

![alt text](images/control-center-ui.png)

## 3. Flink jobs
### 3.1 Configuration
Flink jobs process real-time data streams from Kafka topics to perform fraud detection and statistics analysis.

#### 3.1.1 Prerequisites
- Ensure Kafka and Flink cluster are running
- CDC connector is configured and streaming data to Kafka topics
- Required Python packages are installed:
```bash
pip install -r script/flink/requirements.txt
```

#### 3.1.2 Environment Setup
```bash
# Set environment variables (if needed)
export WEBHOOK_URL="your_webhook_url_for_alerts"
```

### 3.2 Available Flink Jobs

#### 3.2.1 Fraud Detection Job
**Purpose**: Detect suspicious order patterns in real-time
- **File**: `script/flink/fraud_detection/fraud_detection_order.py`
- **Input**: Kafka topic `ecommere-cdc.public.orders`
- **Detection Rules**:
  - **Frequent Orders**: More than 5 orders from same customer within 1 minute
  - **High Value Orders**: Orders exceeding predefined threshold
  - **Suspicious Patterns**: Multiple orders with similar characteristics
- **Output**: 
  - Fraud alerts to webhook (Slack/Discord)
  - Alert logs to file system (`/tmp/flink_output`)

**Features**:
- Real-time stream processing with event time windows
- Watermark handling for late-arriving events
- Custom fraud detection algorithms
- Alert notification system

#### 3.2.2 Revenue Statistics Job
**Purpose**: Calculate real-time revenue metrics and trends
- **File**: `script/flink/statistics/statistics_revenue.py`
- **Input**: Kafka topic `ecommere-cdc.public.orders`
- **Metrics**:
  - **Real-time Revenue**: Total revenue per time window
  - **Order Count**: Number of orders per window
  - **Average Order Value**: Revenue divided by order count
  - **Revenue Trends**: Comparison with previous windows
- **Window**: Tumbling windows (configurable duration)
- **Output**: Processed statistics to Kafka topic or file

**Key Features**:
- Tumbling time windows for revenue aggregation
- Custom reduce functions for efficient calculations
- Window-based statistics with timestamp information
- Scalable processing for high-volume order streams

#### 3.2.3 Top Products Analysis Job
**Purpose**: Identify trending and best-selling products in real-time
- **File**: `script/flink/statistics/statistics_top_product.py`
- **Input**: Kafka topic `ecommere-cdc.public.order_items`
- **Analytics**:
  - **Top Products by Quantity**: Most ordered products
  - **Top Products by Revenue**: Highest revenue generating products
  - **Product Trending**: Products with increasing order frequency
  - **Category Performance**: Sales performance by product category
- **Window**: Configurable time windows (1 minute, 5 minutes, 1 hour)
- **Output**: Top products rankings and trends

**Features**:
- Real-time product ranking algorithms
- Quantity and revenue-based sorting
- Category-wise product analysis
- Configurable ranking windows

### 3.3 Utility Components

#### 3.3.1 Stream Creation (`utils/create_stream.py`)
- Kafka source/sink connector configuration
- Stream environment setup
- Common streaming patterns

#### 3.3.2 Alert System (`utils/sender_alert.py`)
- Webhook integration for real-time alerts
- Support for Slack, Discord, and custom webhooks
- Alert formatting and delivery

#### 3.3.3 Common Utilities (`utils/utils.py`)
- Data parsing functions
- Timestamp assignment for event time processing
- Custom data types and serialization
- Watermark strategies

### 3.4 Running Flink Jobs

#### 3.4.1 Run Individual Jobs
```bash
# Fraud Detection Job
python script/flink/fraud_detection/fraud_detection_order.py

# Revenue Statistics Job  
python script/flink/statistics/statistics_revenue.py

# Top Products Analysis Job
python script/flink/statistics/statistics_top_product.py
```

#### 3.4.2 Run All Jobs
```bash
# Execute all Flink jobs
bash script/flink/run.sh
```

#### 3.4.3 Docker Deployment
```bash
# Deploy Flink jobs using Docker Compose
docker-compose -f script/flink/docker-compose-job-flink.yml up -d
```

### 3.5 Monitoring and Debugging

#### 3.5.1 Flink Web UI
- Access Flink Dashboard: `http://localhost:8081`
- Monitor job status, metrics, and performance
- View task manager and job manager logs

#### 3.5.2 Job Logs
```bash
# Check job execution logs
tail -f /tmp/flink_output/*

# Monitor alert outputs
tail -f logs/fraud_alerts.log
```

#### 3.5.3 Kafka Topic Monitoring
- Access control center via `localhost:9021`
- or run script:  
```bash
# Check processed data in output topics
python script/kafka_producer/check_kafka_topic.py --topic flink_revenue_stats
python script/kafka_producer/check_kafka_topic.py --topic flink_top_products
```

### 3.6 Configuration Options

#### 3.6.1 Job Parameters
- **Window Size**: Configurable time windows (1min, 5min, 1hour)
- **Parallelism**: Adjust based on data volume and cluster resources
- **Checkpointing**: Enable for fault tolerance
- **Watermark Strategy**: Configure for late event handling

#### 3.6.2 Performance Tuning
```python
# Example configuration in job files
env.set_parallelism(4)
env.enable_checkpointing(60000)  # 1 minute checkpoints
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
```

### 3.7 Troubleshooting
#### 3.7.1: CDC PostgreSQL Decimal to Base64 Issue
- Debezium CDC converts PostgreSQL decimal/numeric fields to Base64 strings instead of numbers.
```bash
// Expected: "total_price": 638807.
// Actual:   "total_price": "ARMyI/A="
```
> Solution: Fix Connector Configuration
```bash
{
    "name": "ecommerce-cdc",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        // ... other configs ...
        
        // Add these at connector level (not in transforms)
        "decimal.handling.mode": "double",
        "binary.handling.mode": "bytes",
        "money.fraction.digits": "2",
        
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        // Remove decimal configs from here
    }
}
```bash

