import os
import json
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis

app = FastAPI(title="SignalFlow Fraud API", version="0.1.0")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

def get_redis():
    return redis.from_url(REDIS_URL, decode_responses=True)

@app.get("/health")
def health():
    try:
        r = get_redis()
        r.ping()
        return {"status": "healthy", "redis": "connected", "timestamp": time.time()}
    except Exception as e:
        return {"status": "degraded", "error": str(e), "timestamp": time.time()}

class TransactionRequest(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    currency: str = "USD"
    merchant_id: str = None

@app.post("/ingest")
def ingest_transaction(req: TransactionRequest):
    """Accept transaction for async scoring (optional bypass)"""
    # In production: push to Kafka topic
    return {"status": "queued", "transaction_id": req.transaction_id}

@app.get("/alert/{transaction_id}")
def get_alert(transaction_id: str):
    """Fetch alert by transaction ID (if generated)"""
    try:
        r = get_redis()
        # First try direct lookup by incident_id index
        incident_key = r.get(f"tx:{transaction_id}")
        if not incident_key:
            return {"status": "no_alert", "transaction_id": transaction_id}
        
        alert_json = r.get(f"alert:{incident_key}")
        if not alert_json:
            return {"status": "expired", "transaction_id": transaction_id}
        
        alert = json.loads(alert_json)
        return {
            "status": "alert_found",
            "alert": alert,
            "cache_ttl_remaining": r.ttl(f"alert:{incident_key}")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics")
def get_metrics():
    """Return configured thresholds + model metadata"""
    return {
        "risk_threshold": 0.65,  # Could load from config
        "model_version": "xgboost_fraud_baseline_v1",
        "expected_fp_rate": 0.047,
        "expected_recall": 0.707,
        "timestamp": time.time()
    }

@app.get("/")
def root():
    return {
        "service": "signalflow-fraud-api",
        "docs": "/docs",
        "endpoints": {
            "health": "GET /health",
            "ingest": "POST /ingest",
            "alert": "GET /alert/{transaction_id}",
            "metrics": "GET /metrics"
        }
    }