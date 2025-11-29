import pandas as pd
import numpy as np

class DataValidator:
    def __init__(self, df, dataset_name="dataset"):
        self.df = df
        self.dataset_name = dataset_name
        self.report = {
            "dataset_name": dataset_name,
            "total_records": len(df),
            "passed_checks": 0,
            "failed_checks": 0,
            "failures": []
        }

    def check_schema(self, expected_columns):
        """Check if all expected columns exist."""
        missing_cols = [col for col in expected_columns if col not in self.df.columns]
        if missing_cols:
            self._log_failure("Schema Check", f"Missing columns: {missing_cols}")
        else:
            self._log_success()

    def check_nulls(self, columns, threshold=0.0):
        """Check if null percentage exceeds threshold."""
        for col in columns:
            if col not in self.df.columns:
                continue
            null_count = self.df[col].isnull().sum()
            null_pct = null_count / len(self.df)
            
            if null_pct > threshold:
                self._log_failure(f"Null Check ({col})", 
                                  f"Null percentage {null_pct:.2%} exceeds threshold {threshold:.2%}")
            else:
                self._log_success()

    def check_duplicates(self, subset, threshold=0.0):
        """Check for duplicates in specific columns."""
        dup_count = self.df.duplicated(subset=subset).sum()
        dup_pct = dup_count / len(self.df)
        
        if dup_pct > threshold:
            self._log_failure(f"Duplicate Check ({subset})", 
                              f"Duplicate percentage {dup_pct:.2%} exceeds threshold {threshold:.2%}")
        else:
            self._log_success()

    def check_value_range(self, column, min_val=None, max_val=None):
        """Check if values are within range."""
        if column not in self.df.columns:
            return

        if min_val is not None:
            invalid_min = self.df[self.df[column] < min_val]
            if not invalid_min.empty:
                self._log_failure(f"Range Check Min ({column})", 
                                  f"Found {len(invalid_min)} records below {min_val}")
            else:
                self._log_success()

        if max_val is not None:
            invalid_max = self.df[self.df[column] > max_val]
            if not invalid_max.empty:
                self._log_failure(f"Range Check Max ({column})", 
                                  f"Found {len(invalid_max)} records above {max_val}")
            else:
                self._log_success()

    def check_categorical_values(self, column, allowed_values):
        """Check if values are in the allowed list."""
        if column not in self.df.columns:
            return
            
        invalid_values = self.df[~self.df[column].isin(allowed_values)]
        if not invalid_values.empty:
            unique_invalid = invalid_values[column].unique()
            self._log_failure(f"Category Check ({column})", 
                              f"Found invalid values: {unique_invalid}")
        else:
            self._log_success()

    def _log_success(self):
        self.report["passed_checks"] += 1

    def _log_failure(self, check_name, message):
        self.report["failed_checks"] += 1
        self.report["failures"].append({
            "check": check_name,
            "message": message
        })

    def get_report(self):
        return self.report
