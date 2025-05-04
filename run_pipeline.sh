# Ibrahim Malik x23373385
#!/bin/bash

echo "Started at: $(date)"
echo "Starting Disability Data Pipeline..."
echo "----------------------------------------"

# Activate virtual environment if needed
# source venv/bin/activate

# Step 1: Set up PostgreSQL and MongoDB schema
echo "Setting up database schema..."
python3 db_utils.py

# Step 2: Run ETL ingestion
echo "Ingesting data..."
python3 data_ingestion.py

# # Step 3: Analysis execution
# echo "Running analysis notebook..."
# jupyter nbconvert --execute --to notebook --inplace analysis.ipynb

echo -e "\n----------------------------------------"
echo "Pipeline completed successfully!"
echo "Proceed with analysis."

echo "Completed at: $(date)