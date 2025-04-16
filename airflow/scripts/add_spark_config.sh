#!/usr/bin/env bash

airflow db init

echo "============================"
echo "== Configure Spark config =="
echo "============================"
airflow connections add spark_conn \
    --conn-type spark \
    --conn-host "spark://spark-master:7077" \
    --conn-extra "{\"queue\": \"root.default\", \"deploy-mode\": \"client\"}"
airflow connections add spark_local \
    --conn-type spark \
    --conn-host "local" \
    --conn-extra "{\"queue\": \"root.default\", \"deploy-mode\": \"client\"}"
