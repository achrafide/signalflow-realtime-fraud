#!/usr/bin/env bash
set -e

echo "🔥 Submitting Spark Streaming Job..."
echo "📡 Master: spark://spark-master:7077"
echo "📦 Resolving Kafka connector via --packages..."
echo ""

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.sql.streaming.forceDeleteTempCheckpointFiles=true \
  /opt/spark-app/spark_job.py

echo ""
echo "✅ Spark job finished or terminated."