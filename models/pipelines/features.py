import pandas as pd
import numpy as np
import yaml
from typing import Dict, List

def load_config(path: str = "/mnt/c/Users/achra/OneDrive/Bureau/ai-realtime-system/models/configs/feature_schema.yaml") -> Dict:
    with open(path) as f:
        return yaml.safe_load(f)

def engineer_features(df: pd.DataFrame, config_path: str = None) -> pd.DataFrame:
    config = load_config(config_path) if config_path else load_config()
    df = df.copy()

    # 1. Time features
    df['hour_of_day'] = (df['TransactionDT'] % 86400) // 3600
    df['day_of_week'] = (df['TransactionDT'] // 86400) % 7

    # 2. Amount transformation
    df['transaction_amt_log'] = np.log1p(df['TransactionAmt'])

    # 3. Basic velocity proxies (batch approximation of rolling windows)
    df['user_freq_24h'] = df.groupby('card1')['TransactionID'].transform('count')
    df['merchant_freq_24h'] = df.groupby('P_emaildomain')['TransactionID'].transform('count').fillna(0)

    # 4. Missing value indicators
    for col in config['missing_indicators']:
        df[f'is_missing_{col}'] = df[col].isna().astype(int)

    # 5. Imputation
    for col, strategy in config['imputation'].items():
        if col == 'default': continue
        val = df[col].median() if strategy == 'median' else float(strategy)
        df[col] = df[col].fillna(val)

    # 6. Simple categorical encoding (target/label)
    for cat_col in ['card4', 'P_emaildomain']:
        df[f'{cat_col}_encoded'] = df[cat_col].astype('category').cat.codes

    # 7. Final selection & safe fill
    feature_cols = config['feature_columns']
    df_out = df[feature_cols].fillna(0)
    
    return df_out