#!/usr/bin/env bash
set -e

echo "🚀 Setting up AI Realtime System..."

# 1. Start all services
docker compose up -d

# 2. Wait for Kafka to be healthy
echo "⏳ Waiting for Kafka to initialize..."
until docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092 &>/dev/null; do
  sleep 2
done
echo "✅ Kafka is ready!"

# 3. Create topic (idempotent: safe to run multiple times)
echo "📦 Creating Kafka topic 'raw_transactions'..."
docker compose exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic raw_transactions \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists
echo "✅ Topic 'raw_transactions' is ready!"

# 4. Print status & next steps
echo ""
echo "🔍 System Status:"
docker compose ps
echo ""
echo "🌐 Quick Links:"
echo "   Spark UI: http://localhost:8080"
echo "   Redis:    redis://localhost:6379"
echo ""
echo "✅ Setup complete! Run './run-spark.sh' to start the streaming job."