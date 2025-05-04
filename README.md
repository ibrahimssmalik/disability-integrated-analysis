# Exploring Socioeconomic Drivers of Disability Prevalence Using Distributed Big Data Systems

This project investigates socioeconomic drivers of disability prevalence across the United States, with a focus on important factors like income, education, and access to healthcare. The project integrates data from multiple sources using big data systems, including PostgreSQL, MongoDB, and Azure Blob Storage, and utilizes tools like PySpark and Pandas for analysis. In order to predict disability prevalence based on socioeconomic indicators, machine learning techniques are also applied.

## Project Structure

- `db_utils.py`: Contains helper classes for database setup and interactions with PostgreSQL and MongoDB.
- `data_ingestion.py`: For retrieving datasets from Azure Blob Storage and storing them in PostgreSQL and MongoDB.
- `data_ingestion.ipynb`: Jupyter Notebook that performs data ingestion, transformation, and storage in databases.
- `analysis.ipynb`: Jupyter Notebook that performs data analysis and visualizations, including machine learning models with results.
- `run_pipeline.sh`: Shell script to automate the entire data ingestion pipeline from database setup to data processing.

## Prerequisites

Before running the project, make sure you have the following installed:
- Python 3.x
- PostgreSQL and MongoDB databases set up
- Apache Spark with PySpark installed

## Setup and Running the Project

### 1. Set up the virtual environment
Create and activate a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
```

### 2. Install the required libraries
```bash
pip install -r requirements.txt
```
### 3. Run the pipeline
To execute the pipeline and ingest the data, run the following command:
```bash
bash run_pipeline.sh
```
This will:
- Set up the PostgreSQL and MongoDB databases.
- Retrieve datasets from Azure Blob Storage if not already present.
- Ingest the data into the databases.
- Process and analyze the data.

### 4. Analyze the data
To run the analysis, open the `analysis.ipynb` notebook and run the cells to explore the data, create visualizations, and train machine learning models.

## Datasets
The following datasets are used in the project:
- Disability Prevalence Data: Disability statistics across the U.S. states.
- Income Data: U.S. income statistics by state.
- Healthcare Access: Percentage of adults without a personal doctor by state.
- Education Data: Percentage of U.S. adults with a bachelor's degree by state (retrieved via an API).

## Results
The project analyzes how socioeconomic factors such as education, income, and healthcare access correlate with disability prevalence. The results include visualizations, statistical analysis, and machine learning model predictions.

## Authors
Ibrahim Malik (x23373385)
National College of Ireland
Master of Data Analytics - Data Intensive Scalable Systems

## License
This project is licensed under the MIT License.
