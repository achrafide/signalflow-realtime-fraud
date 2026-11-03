import os
import json
import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
import yaml
import mlflow
from typing import Dict
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, average_precision_score, precision_recall_curve, confusion_matrix
from features import engineer_features

def load_config(path: str = "/mnt/c/Users/achra/OneDrive/Bureau/ai-realtime-system/models/configs/training_pipeline.yaml") -> Dict:
    with open(path) as f:
        return yaml.safe_load(f)

def load_data(train_path: str, subsample: float = 1.0) -> pd.DataFrame:
    """Load ONLY the labeled training data. Test set is for inference, not evaluation."""
    base_cols = [
        "TransactionID", "TransactionDT", "TransactionAmt",
        "card1", "card2", "card4", "dist1", "P_emaildomain"
    ]
    dtype_map = {
        "TransactionID": "int32", "TransactionDT": "int32",
        "TransactionAmt": "float32", "card1": "float32",
        "card2": "float32", "card4": "category",
        "dist1": "float32", "P_emaildomain": "category"
    }
    
    print("📥 Loading labeled data...")
    df = pd.read_csv(
        train_path, 
        usecols=base_cols + ["isFraud"], 
        dtype={**dtype_map, "isFraud": "int8"}
    )
    
    if subsample < 1.0:
        df = df.sample(frac=subsample, random_state=42)
        
    print(f"✅ Loaded {df.shape[0]} rows | Fraud rate: {df['isFraud'].mean()*100:.2f}%")
    return df
def prepare_data(df: pd.DataFrame, cfg: Dict) -> tuple:
    """Engineer features + stratified train/test split"""
    target = cfg['data']['target_col']
    
    X = engineer_features(df.drop(columns=[target]))
    y = df[target].astype(int)
    
    # Stratified split guarantees fraud exists in BOTH sets
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, 
        test_size=cfg['training']['test_size'], 
        random_state=cfg['training']['random_state'], 
        stratify=y  # ← CRITICAL for imbalanced data
    )
    
    print(f"📊 Split: Train {len(X_train)} | Test {len(X_test)}")
    return X_train, X_test, y_train, y_test

def train_model(X_train, y_train, cfg: Dict) -> xgb.XGBClassifier:
    with open("/mnt/c/Users/achra/OneDrive/Bureau/ai-realtime-system/models/configs/hyperparams.yaml") as f:
        params = yaml.safe_load(f)
        
    # Automatic class imbalance handling
    scale_pos = (y_train == 0).sum() / max((y_train == 1).sum(), 1)
    params['scale_pos_weight'] = round(scale_pos, 2)
    
    model = xgb.XGBClassifier(**params, eval_metric='logloss')
    model.fit(
        X_train, y_train,
        eval_set=[(X_train, y_train)],
        verbose=params.get('verbose', False)
    )
    return model

def evaluate_model(model, X_test, y_test, cfg: Dict) -> Dict:
    y_prob = model.predict_proba(X_test)[:, 1]
    threshold = cfg['evaluation']['threshold']
    y_pred = (y_prob > threshold).astype(int)
    
    # Extract scalar components from confusion matrix
    tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
    
    metrics = {
        'roc_auc': float(roc_auc_score(y_test, y_prob)),
        'pr_auc': float(average_precision_score(y_test, y_prob)),
        'threshold_used': float(threshold),
        'true_negatives': int(tn),
        'false_positives': int(fp),
        'false_negatives': int(fn),
        'true_positives': int(tp),
    }
    
    # Precision @ Recall target
    precision, recall, _ = precision_recall_curve(y_test, y_prob)
    target_recall = cfg['evaluation']['precision_recall_target']
    idx = np.argmin(np.abs(recall - target_recall))
    metrics['precision_at_target_recall'] = float(precision[idx])
    
    return metrics

def save_artifacts(model, metrics, cfg: Dict, output_dir: str = "/mnt/c/Users/achra/OneDrive/Bureau/ai-realtime-system/models/artifacts/baseline"):
    os.makedirs(output_dir, exist_ok=True)
    
    model_path = os.path.join(output_dir, "xgboost_fraud_baseline.pkl")
    joblib.dump(model, model_path)
    
    metadata = {
        'model_type': 'XGBoostClassifier',
        'metrics': metrics,
        'feature_config': '/mnt/c/Users/achra/OneDrive/Bureau/ai-realtime-system/models/configs/feature_schema.yaml',
        'timestamp': pd.Timestamp.now().isoformat(),
        'version': 'v1.0-baseline'
    }
    with open(os.path.join(output_dir, "metadata.json"), 'w') as f:
        json.dump(metadata, f, indent=2)
        
    print(f"✅ Saved to {output_dir}")
    return model_path

def main():
    cfg = load_config()
    print("📥 Loading labeled data...")
    df = load_data(cfg['data']['train_path'], cfg['data']['subsample_fraction'])
    
    print("🔧 Engineering features & splitting...")
    X_train, X_test, y_train, y_test = prepare_data(df, cfg)
    
    print("📈 Training XGBoost...")
    with mlflow.start_run(run_name="baseline_fraud_v1"):
        model = train_model(X_train, y_train, cfg)
        metrics = evaluate_model(model, X_test, y_test, cfg)
        mlflow.log_params(cfg)
        mlflow.log_metrics(metrics)
        
    print("💾 Saving artifacts...")
    save_artifacts(model, metrics, cfg)
    print("📊 Metrics:", json.dumps(metrics, indent=2))

if __name__ == "__main__":
    main()