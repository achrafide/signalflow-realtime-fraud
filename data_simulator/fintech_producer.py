#!/usr/bin/env python3
"""
Fintech Transaction Simulator for SignalFlow Fraud Detection
Generates realistic payment events with embedded fraud patterns for testing.
"""
import json
import time
import random
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer

# ──────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "raw_transactions"
FRAUD_RATE = 0.05  # 5% of transactions are fraudulent patterns

# Realistic value ranges
AMOUNT_NORMAL = (5.0, 500.0)
AMOUNT_FRAUD = (500.0, 10000.0)
HOURS_NORMAL = list(range(8, 23))  # 8 AM - 11 PM
HOURS_FRAUD = [0, 1, 2, 3, 4, 5]    # Late night = suspicious

# Mock reference data
MERCHANTS_LEGIT = ["amazon", "walmart", "starbucks", "uber", "netflix", "spotify"]
MERCHANTS_FRAUD = ["crypto_exchange_xyz", "offshore_merchant", "new_vendor_123"]
EMAIL_DOMAINS_LEGIT = ["gmail.com", "yahoo.com", "outlook.com", "company.com"]
EMAIL_DOMAINS_FRAUD = ["tempmail.com", "guerrillamail.com", "10minutemail.com"]
CARD_TYPES = ["visa", "mastercard", "amex", "discover"]

# ──────────────────────────────────────────────────────────────
# DATA GENERATION
# ──────────────────────────────────────────────────────────────
def generate_legit_transaction() -> dict:
    """Generate a normal, low-risk transaction"""
    return {
        "transaction_id": f"tx_{uuid.uuid4().hex[:12]}",
        "user_id": f"user_{random.randint(1, 1000)}",
        "amount": round(random.uniform(*AMOUNT_NORMAL), 2),
        "currency": random.choice(["USD", "EUR", "GBP"]),
        "merchant_id": random.choice(MERCHANTS_LEGIT),
        "timestamp": datetime.now().isoformat(),
        "hour_of_day": random.choice(HOURS_NORMAL),
        "card1": random.randint(10000, 99999),  # Mock card token
        "card2": random.randint(100, 999),       # Mock CVV hash
        "card4": random.choice(CARD_TYPES),
        "dist1": round(random.uniform(0.1, 100.0), 2),  # Mock distance
        "P_emaildomain": random.choice(EMAIL_DOMAINS_LEGIT)
    }

def generate_fraud_transaction() -> dict:
    """Generate a high-risk transaction pattern"""
    pattern = random.choice(["high_amount", "late_hour", "suspicious_merchant", "temp_email"])
    
    base = {
        "transaction_id": f"tx_{uuid.uuid4().hex[:12]}",
        "user_id": f"user_{random.randint(1, 1000)}",
        "currency": "USD",
        "timestamp": datetime.now().isoformat(),
        "card1": random.randint(10000, 99999),
        "card2": random.randint(100, 999),
        "card4": random.choice(CARD_TYPES),
        "dist1": round(random.uniform(0.1, 100.0), 2),
    }
    
    if pattern == "high_amount":
        base.update({
            "amount": round(random.uniform(*AMOUNT_FRAUD), 2),
            "merchant_id": random.choice(MERCHANTS_LEGIT),
            "hour_of_day": random.choice(HOURS_NORMAL),
            "P_emaildomain": random.choice(EMAIL_DOMAINS_LEGIT)
        })
    elif pattern == "late_hour":
        base.update({
            "amount": round(random.uniform(*AMOUNT_NORMAL), 2),
            "merchant_id": random.choice(MERCHANTS_LEGIT),
            "hour_of_day": random.choice(HOURS_FRAUD),
            "P_emaildomain": random.choice(EMAIL_DOMAINS_LEGIT)
        })
    elif pattern == "suspicious_merchant":
        base.update({
            "amount": round(random.uniform(*AMOUNT_NORMAL), 2),
            "merchant_id": random.choice(MERCHANTS_FRAUD),
            "hour_of_day": random.choice(HOURS_NORMAL),
            "P_emaildomain": random.choice(EMAIL_DOMAINS_LEGIT)
        })
    else:  # temp_email
        base.update({
            "amount": round(random.uniform(*AMOUNT_NORMAL), 2),
            "merchant_id": random.choice(MERCHANTS_LEGIT),
            "hour_of_day": random.choice(HOURS_NORMAL),
            "P_emaildomain": random.choice(EMAIL_DOMAINS_FRAUD)
        })
    
    return base

def generate_transaction() -> dict:
    """Main generator: randomly pick legit or fraud based on FRAUD_RATE"""
    if random.random() < FRAUD_RATE:
        return generate_fraud_transaction()
    return generate_legit_transaction()

# ──────────────────────────────────────────────────────────────
# KAFKA PRODUCER
# ──────────────────────────────────────────────────────────────
def create_producer() -> KafkaProducer:
    """Create Kafka producer with retry logic"""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                request_timeout_ms=30000
            )
            # ✅ Producer instantiation = successful bootstrap connection
            print(f"✅ Connected to Kafka at {KAFKA_BOOTSTRAP}", flush=True)
            return producer
        except Exception as e:
            print(f"⏳ Kafka not ready: {e}, retrying in 5s...", flush=True)
            time.sleep(5)

def main():
    print("🚀 SignalFlow Fintech Simulator starting...", flush=True)
    producer = create_producer()
    
    print(f"📤 Publishing to topic: {TOPIC} | Fraud rate: {FRAUD_RATE*100:.1f}%", flush=True)
    
    count = 0
    fraud_count = 0
    
    while True:
        try:
            event = generate_transaction()
            is_fraud = event["merchant_id"] in MERCHANTS_FRAUD or \
                      event["P_emaildomain"] in EMAIL_DOMAINS_FRAUD or \
                      event["hour_of_day"] in HOURS_FRAUD or \
                      event["amount"] > 1000
            
            future = producer.send(TOPIC, value=event)
            future.get(timeout=10)  # Wait for ack
            producer.flush()
            
            count += 1
            if is_fraud:
                fraud_count += 1
            
            # Log progress every 10 events
            if count % 10 == 0:
                print(f"✅ Sent {count} events | Fraud: {fraud_count} ({fraud_count/count*100:.1f}%) | Latest: {event['transaction_id']}", flush=True)
            
            # Variable delay to simulate real traffic bursts
            time.sleep(random.uniform(0.5, 2.0))
            
        except KeyboardInterrupt:
            print("\n🛑 Shutting down...", flush=True)
            producer.close()
            break
        except Exception as e:
            print(f"⚠️ Send error: {e}", flush=True)
            time.sleep(5)

if __name__ == "__main__":
    main()