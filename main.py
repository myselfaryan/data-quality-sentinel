import pandas as pd
import os
import shutil
from src.validator import DataValidator
from src.alert_handler import AlertHandler
from src.generate_data import generate_data

# Configuration
DATA_FILE = 'data/raw/transactions.csv'
PROCESSED_DIR = 'data/processed'
QUARANTINE_DIR = 'data/quarantine'
EXPECTED_COLUMNS = ['transaction_id', 'user_id', 'amount', 'timestamp', 'status']
ALLOWED_STATUSES = ['COMPLETED', 'PENDING', 'FAILED']

def main():
    print("--- Starting Data Quality Pipeline ---")
    
    # 1. Ingest Data (Generate if not exists for demo)
    if not os.path.exists(DATA_FILE):
        print("Data file not found. Generating new data...")
        generate_data()
    
    try:
        df = pd.read_csv(DATA_FILE)
        print(f"Loaded {len(df)} records from {DATA_FILE}")
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    # 2. Validate Data
    print("Running validation checks...")
    validator = DataValidator(df, dataset_name="Transactions")
    
    # Define Rules
    validator.check_schema(EXPECTED_COLUMNS)
    validator.check_nulls(['user_id', 'transaction_id', 'amount'], threshold=0.0) # Strict no nulls
    validator.check_duplicates(['transaction_id'], threshold=0.0) # Strict no dupes
    validator.check_value_range('amount', min_val=0.0) # No negative amounts
    validator.check_categorical_values('status', ALLOWED_STATUSES)
    
    report = validator.get_report()
    
    # 3. Handle Results & Alerting
    alerter = AlertHandler()
    
    if report['failed_checks'] > 0:
        print("Validation FAILED. Moving data to quarantine.")
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        shutil.move(DATA_FILE, os.path.join(QUARANTINE_DIR, f"transactions_bad_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.csv"))
        
        # Send Critical Alert
        alerter.send_alert(report, level='CRITICAL')
    else:
        print("Validation PASSED. Moving data to processed.")
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        shutil.move(DATA_FILE, os.path.join(PROCESSED_DIR, f"transactions_clean_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.csv"))
        
        # Send Info Alert
        alerter.send_alert(report, level='INFO')

    print("--- Pipeline Finished ---")

if __name__ == "__main__":
    main()
