#!/bin/bash

python script/flink/fraud_detection/fraud_detection_order.py
python script/flink/statistics/statistics_revenue.py
python script/flink/statistics/statistics_top_product.py