# Data Quality & Monitoring System

A robust, production-ready Data Quality (DQ) framework designed to ensure data reliability in ETL pipelines. This system ingests raw data, performs comprehensive validation checks (schema, nulls, duplicates, business logic), and automatically routes data to either a "processed" or "quarantine" bucket based on quality thresholds. It also features a real-time alerting mechanism.

## 🚀 Features

- **Automated Data Generation**: Synthetic data generator with configurable "dirty" data injection for testing.
- **Comprehensive Validation Suite**:
  - **Schema Validation**: Ensures column presence and types.
  - **Null Checks**: Configurable thresholds for missing values.
  - **Duplicate Detection**: Identifies primary key violations.
  - **Business Logic Rules**: Validates value ranges (e.g., non-negative amounts) and categorical constraints.
- **Data Routing**:
  - ✅ **Clean Data**: Moved to `data/processed/` for downstream consumption.
  - ❌ **Bad Data**: Isolated in `data/quarantine/` for manual inspection.
- **Alerting System**: Simulates Slack/Email alerts with detailed failure reports (Console output + Log file).

## 🛠️ Tech Stack

- **Language**: Python 3.x
- **Libraries**: Pandas, NumPy, Colorama
- **Architecture**: Modular design (`Validator`, `AlertHandler`, `Pipeline Orchestrator`)

## 📂 Project Structure

```
.
├── data/
│   ├── raw/            # Landing zone for incoming data
│   ├── processed/      # Clean data ready for use
│   └── quarantine/     # Data that failed validation
├── src/
│   ├── generate_data.py # Generates synthetic dataset with errors
│   ├── validator.py     # Core validation logic
│   ├── alert_handler.py # Handles notifications
├── main.py             # Pipeline entry point
├── run_pipeline.sh     # Helper script to run the full flow
├── requirements.txt    # Dependencies
└── README.md           # Documentation
```

## ⚡ How to Run

1. **Setup Environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Run the Pipeline**:
   ```bash
   chmod +x run_pipeline.sh
   ./run_pipeline.sh
   ```

## 📊 Sample Output

```text
--- Starting Data Quality Pipeline ---
Loaded 1010 records from data/raw/transactions.csv
Running validation checks...
Validation FAILED. Moving data to quarantine.

[2023-10-27 10:00:00] DATA QUALITY ALERT - Level: CRITICAL
==================================================
Dataset: Transactions
Total Records: 1010
Passed Checks: 3
Failed Checks: 4

FAILURE DETAILS:
- Null Check (user_id): Null percentage 5.00% exceeds threshold 0.00%
- Duplicate Check (['transaction_id']): Duplicate percentage 1.00% exceeds threshold 0.00%
- Range Check Min (amount): Found 20 records below 0.0
- Category Check (status): Found invalid values: ['UNKNOWN_STATUS']
==================================================
--- Pipeline Finished ---
```

## 📝 Future Improvements

- Integration with **Great Expectations** for more complex rule definitions.
- **Slack/PagerDuty** integration for real-time notifications.
- **Airflow** DAG for scheduling.
- **Dashboard** for tracking DQ metrics over time.
