#!/bin/bash

# Activate virtual environment
source venv/bin/activate

# Generate fresh data (optional, main.py handles it too but this forces a refresh)
echo "Generating fresh data..."
python src/generate_data.py

# Run the pipeline
echo "Running Data Quality Pipeline..."
python main.py
