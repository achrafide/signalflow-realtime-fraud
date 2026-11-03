# streaming/spark_job.py — SignalFlow real-time FRAUD SERVING (A-lite, honest v1)
# Reads ONLY kafka_source.yaml + feature_schema.yaml + thresholds.yaml + registry pointer.
# Never reads training_pipeline.yaml / hyperparams.yaml (offline-only). Model = opaque blob behind score().
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
import redis, json, os, time, yaml, joblib
from datetime import datetime

CONFIG_DIR = os.getenv("CONFIG_DIR", "/opt/config")    # = ./models/configs (unified mount)
MODEL_DIR  = os.getenv("MODEL_DIR",  "/opt/models")    # = ./models/artifacts (unified mount)
REDIS_URL  = os.getenv("REDIS_URL", "redis://redis:6379/0")

def _yaml(name):
    with open(os.path.join(CONFIG_DIR, name)) as f:
        return yaml.safe_load(f)

KAFKA   = _yaml("kafka_source.yaml")
FEATSCH = _yaml("feature_schema.yaml")
THRESH  = _yaml("thresholds.yaml")

# ── 1:1 guard: derivations must cover feature_columns exactly, same order ──────
_fc = FEATSCH["feature_columns"]; _dv = FEATSCH["derivations"]
if set(_fc) != set(_dv) or len(_fc) != len(_dv):
    raise ValueError(
        f"feature_schema skew: columns={_fc} vs derivations={list(_dv)}. "
        f"missing={set(_fc)-set(_dv)} extra={set(_dv)-set(_fc)}")

_TYPE = {"string": StringType(), "double": DoubleType(), "float": DoubleType(),
         "int": IntegerType(), "long": LongType()}
EVENT_SCHEMA = StructType([StructField(s["name"], _TYPE[s["type"]], True) for s in KAFKA["schema"]])
KEY_FIELD = KAFKA["source"]["key_field"]

spark = SparkSession.builder \
    .appName("SignalFlow-FraudServing") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointFiles", "true") \
    .config("spark.sql.shuffle.partitions", "2").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA["source"]["bootstrap"]) \
    .option("subscribe", KAFKA["source"]["topic"]) \
    .option("startingOffsets", KAFKA["source"]["starting_offsets"]).load()

parsed_df = raw_df.select(
    F.col("key").cast("string").alias("event_key"),
    F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"),
    F.current_timestamp().alias("spark_ingested_at")
).select("event.*", "spark_ingested_at")

def _expr(d):
    op, src = d["op"], d.get("src")
    if op == "passthrough":  return F.col(src)
    if op == "log1p":        return F.log1p(F.col(src))
    if op == "to_dayofweek":  # resilient ISO parse: strip fractional seconds, keep the 'T'
        ts_no_micro = F.split(F.col(src), r"\.")[0]
        return F.dayofweek(F.to_timestamp(ts_no_micro, "yyyy-MM-dd'T'HH:mm:ss"))
    if op == "is_null":      return F.col(src).isNull().cast("int")
    if op == "placeholder":  return F.lit(float(d["value"]))
    raise ValueError(f"unknown derivation op: {op}")

features_df = parsed_df
for name in _fc:
    features_df = features_df.withColumn(name, _expr(_dv[name]).cast("double"))

class ArtifactLoader:
    """Stream never interprets the blob; it only calls score(). Phase-2 swaps contents (xgboost->rules) here."""
    def __init__(self): self._ptr_mtime = -1.0; self._artifact = None
    def _load(self):
        ptr = os.path.join(MODEL_DIR, "pointer.json")
        if not os.path.exists(ptr):
            print("⚠️ No registry pointer — falling back to baseline artifact (v1 dev mode). "
                  "Promote via models/evaluations before prod.", flush=True)
            self._artifact = {"type": "xgboost",
                "model": joblib.load(os.path.join(MODEL_DIR, "baseline", "xgboost_fraud_baseline.pkl")),
                "version": "v1.0-baseline-fallback"}
            self._ptr_mtime = -2.0; return
        meta = json.load(open(ptr)); ver = meta["active_version"]; vdir = os.path.join(MODEL_DIR, ver)
        manifest = yaml.safe_load(open(os.path.join(vdir, "manifest.yaml")))
        if manifest["type"] == "xgboost":
            self._artifact = {"type": "xgboost", "model": joblib.load(os.path.join(vdir, "model.pkl")), "version": ver}
        elif manifest["type"] == "rules":
            self._artifact = {"type": "rules", "path": vdir, "version": ver}   # phase-2
        else: raise ValueError(f"unknown artifact type: {manifest['type']}")
        self._ptr_mtime = os.path.getmtime(ptr)
        print(f"📦 Loaded artifact {ver} (type={manifest['type']})", flush=True)
    def get(self):
        ptr = os.path.join(MODEL_DIR, "pointer.json")
        if self._artifact is None or (os.path.exists(ptr) and os.path.getmtime(ptr) != self._ptr_mtime):
            self._load()
        return self._artifact

def score(X, artifact):
    """THE seam. Returns P(fraud) per row. Rule engine plugs in here, phase-2."""
    if artifact["type"] == "xgboost": return artifact["model"].predict_proba(X)[:, 1]
    if artifact["type"] == "rules":   raise NotImplementedError("Phase-2 rule engine; seam reserved.")
    raise ValueError(artifact["type"])

LOADER = ArtifactLoader(); A = THRESH["alerting"]
THRESHOLD  = float(A["risk_threshold"]); TTL = int(A.get("ttl_seconds", 300))
MIN_AMT    = float(A.get("min_amount_trigger", 0.0)); COOLDOWN_S = int(A.get("cooldown_minutes", 0)) * 60
EVIDENCE   = list(A.get("evidence_fields", [])); LABEL_HIGH = A.get("verdict_labels", {}).get("high", "review_transaction")
_cooldown = {}

def score_and_alert(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"[Batch {batch_id}] ⏸️ No data", flush=True); return
    pdf = batch_df.toPandas(); X = pdf[_fc].values.astype(float)
    probs = score(X, LOADER.get()); r = redis.from_url(REDIS_URL, decode_responses=True)
    now = time.time(); alerted = 0
    for i, row in pdf.iterrows():
        p = float(probs[i]); uid = row["user_id"]
        if p < THRESHOLD or row["amount"] < MIN_AMT: continue
        if COOLDOWN_S and now - _cooldown.get(uid, 0) < COOLDOWN_S: continue
        inc = f"alert_{row[KEY_FIELD]}_{int(now)}"
        ev = {k: row[k] for k in EVIDENCE if k in row}   # defensive: skips dangling velocity_1h_count
        alert = {"incident_id": inc, "transaction_id": row[KEY_FIELD], "user_id": uid,
                 "amount": float(row["amount"]), "risk_score": round(p, 4), "threshold_used": THRESHOLD,
                 "evidence": ev, "recommended_action": LABEL_HIGH,
                 "model_version": LOADER.get()["version"], "timestamp": datetime.now().isoformat()}
        r.setex(f"tx:{row[KEY_FIELD]}", TTL, inc); r.setex(f"alert:{inc}", TTL, json.dumps(alert))
        _cooldown[uid] = now; alerted += 1
    print(f"[Batch {batch_id}] " + (f"✅ Alerted {alerted} transactions" if alerted else f"✅ Scored {len(pdf)}, no alerts"), flush=True)

query = features_df.writeStream.foreachBatch(score_and_alert).outputMode("append") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/signalflow-fraud").start()
print(f"🔌 SignalFlow fraud serving active | topic={KAFKA['source']['topic']} | threshold={THRESHOLD} | ttl={TTL}s", flush=True)
query.awaitTermination()