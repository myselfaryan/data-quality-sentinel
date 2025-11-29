import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

def generate_data(num_records=1000):
    print("Generating synthetic data...")
    
    # Base data
    data = {
        'transaction_id': [f'TXN_{i}' for i in range(num_records)],
        'user_id': [random.randint(1000, 9999) for _ in range(num_records)],
        'amount': [round(random.uniform(10.0, 1000.0), 2) for _ in range(num_records)],
        'timestamp': [datetime.now() - timedelta(days=random.randint(0, 30)) for _ in range(num_records)],
        'status': [random.choice(['COMPLETED', 'PENDING', 'FAILED']) for _ in range(num_records)]
    }
    
    df = pd.DataFrame(data)
    
    # Inject Data Quality Issues
    
    # 1. Nulls in user_id (approx 5%)
    mask_null = np.random.choice([True, False], size=num_records, p=[0.05, 0.95])
    df.loc[mask_null, 'user_id'] = np.nan
    
    # 2. Duplicates in transaction_id (take first 10 and repeat them)
    duplicates = df.head(10).copy()
    df = pd.concat([df, duplicates], ignore_index=True)
    
    # 3. Invalid amounts (negative values)
    mask_neg = np.random.choice([True, False], size=len(df), p=[0.02, 0.98])
    df.loc[mask_neg, 'amount'] = df.loc[mask_neg, 'amount'] * -1
    
    # 4. Invalid status
    mask_status = np.random.choice([True, False], size=len(df), p=[0.03, 0.97])
    df.loc[mask_status, 'status'] = 'UNKNOWN_STATUS'
    
    # Save
    output_path = 'data/raw/transactions.csv'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Data generated at {output_path} with {len(df)} records (including injected errors).")

if __name__ == "__main__":
    generate_data()
