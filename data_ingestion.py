# Ibrahim Malik x23373385

#!/usr/bin/env python3
import logging
import os
import warnings
import requests
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
from pymongo import MongoClient
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, expr
from pyspark.sql import Row
from db_utils import PostgresDBHelper, MongoDBHelper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# suppress warnings
warnings.filterwarnings("ignore")

def initialize_spark():
    """Initialize and return a Spark session."""
    try:
        # initialize Spark session for PostgreSQL
        spark = SparkSession.builder \
            .appName("PostgresIntegration") \
            .config("spark.jars", "/home/ibrahimssmalik/Downloads/postgresql-42.6.0.jar") \
        .getOrCreate()
        logger.info("Spark session initialized successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to initialize Spark: {str(e)}")
        raise

def initialize_postgres():
    """Initialize and return PostgreSQL connection and helper."""
    try:
        pg_conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="disability_db",
            user="ibrahimssmalik",
            password="virtualbox2025"
        )
        logger.info("Connected to PostgreSQL")
        return pg_conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
        raise

def initialize_mongodb(spark):
    """Initialize and return MongoDB helper."""
    try:
        mongo_conn = "mongodb://ibrahimssmalik:virtualbox2025@localhost:27017"
        logger.info("Connected to MongoDB")
        return mongo_conn
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")
        raise

def download_file(file_name):
    """Download file from Azure blob storage if it doesn't exist locally."""
    try:
        sas_token = "ADD SAS TOKEN HERE"
        file_url = f"https://datasets4disability.blob.core.windows.net/datasets/{file_name}?{sas_token}"
        
        if not os.path.exists(file_name):
            logger.info(f"Downloading {file_name}...")
            r = requests.get(file_url)
            r.raise_for_status() # raise exception for HTTP errors
            with open(file_name, "wb") as f:
                f.write(r.content)
            logger.info(f"{file_name} downloaded successfully")
        else:
            logger.info(f"{file_name} already exists. Skipping download.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download {file_name}: {str(e)}")
        raise

def process_disability_data(spark, db):
    """Process and load disability data."""
    try:
        file_name = "DHDS_2025.csv"
        download_file(file_name)
        
        logger.info("Loading disability data...")
        disability_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_name)
        
        # data transformation
        df_filtered = disability_df.filter(disability_df["Data_Value"].isNotNull())
        
        disability_stats_df = df_filtered.select(
            col("Year").cast("int").alias("year"),
            col("LocationAbbr").alias("state"),
            col("LocationDesc").alias("state_name"),
            col("Category").alias("category"),
            col("Indicator").alias("indicator"),
            col("Response").alias("response"),
            col("Data_Value_Type").alias("data_value_type"),
            col("Data_Value").cast("float").alias("data_value"),
            col("Number").cast("float").alias("number"),
            col("WeightedNumber").cast("int").alias("weightednumber"),
            col("StratificationCategory1").alias("strat_category1"),
            col("Stratification1").alias("strat_value1"),
            col("StratificationCategory2").alias("strat_category2"),
            col("Stratification2").alias("strat_value2"),
        )
        
        # log data metrics
        logger.info(f"Disability data record count: {disability_stats_df.count()}")
        
        # add tables to database
        logger.info("Writing disability data to PostgreSQL...")
        db.add_table('disability_stats', disability_stats_df)
        
        # process categories
        categories_df = df_filtered.select("Category").distinct().withColumnRenamed("Category", "category_name")
        db.add_table('categories', categories_df)
        
        # process indicators
        indicators_df = df_filtered.select("Indicator").distinct().withColumnRenamed("Indicator", "indicator_name")
        db.add_table('indicators', indicators_df)
        
        # process stratifications
        stratifications_df = df_filtered.select(
            col("StratificationCategory1").alias("strat_category"),
            col("Stratification1").alias("strat_value")
        ).distinct()
        db.add_table('stratifications', stratifications_df)
        
        logger.info("Disability data processing completed")
    except Exception as e:
        logger.error(f"Error processing disability data: {str(e)}")
        raise

def process_income_data(spark, db):
    """Process and load income data."""
    try:
        file_name = "Income_Data.csv"
        download_file(file_name)
        
        logger.info("Loading income data...")
        income_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_name)
        
        # log data metrics
        logger.info(f"Income data record count: {income_df.count()}")
        
        # add tables to database
        logger.info("Writing income data to PostgreSQL...")
        db.add_table('income_data', income_df)
        
        logger.info("Income data processing completed")
    except Exception as e:
        logger.error(f"Error processing income data: {str(e)}")
        raise

def process_health_data(spark, db):
    """Process and load health data."""
    try:
        file_name = "Health_Data.csv"
        download_file(file_name)
        
        logger.info("Loading health data...")
        health_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_name)
        
        # log data metrics
        logger.info(f"Health data record count: {health_df.count()}")
        
        # add tables to database
        logger.info("Writing health data to PostgreSQL...")
        db.add_table('health_data', health_df)
        
        logger.info("Health data processing completed")
    except Exception as e:
        logger.error(f"Error processing health data: {str(e)}")
        raise

def process_education_data(spark, db, db_mongo):
    """Process and load education data from Census API to MongoDB and PostgreSQL."""
    try:
        logger.info("Fetching education data from Census API...")
        
        # list to store all data rows
        all_rows = []

        # loop through years 2015–2022
        for year in range(2015, 2023):
            try:
                url = f"https://api.census.gov/data/{year}/acs/acs5/subject?get=NAME,S1501_C02_014E&for=state:*"
                logger.info(f"Fetching data for year {year}...")
                
                response = requests.get(url, timeout=30)
                response.raise_for_status()  # Raise exception for HTTP errors
                
                json_data = response.json()
                headers = json_data[0]
                rows = json_data[1:]

                for row in rows:
                    doc = {
                        "year": year,
                        "state": row[0],
                        "state_code": row[2],
                        "percent_bachelors_or_higher": float(row[1]) if row[1] not in ("", None) else None
                    }
                    all_rows.append(doc)
                
                logger.info(f"Successfully fetched {len(rows)} records for year {year}")
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch data for {year}: {str(e)}")
                # Continue with next year instead of stopping the entire process
                continue
        
        # insert data into MongoDB
        if all_rows:
            logger.info(f"Writing {len(all_rows)} education records to MongoDB...")
            db_mongo.write_collection("education_data", all_rows)
            
            # verify data was written
            collections = db_mongo.list_collections()
            logger.info(f"MongoDB collections: {collections}")
            
            # read from MongoDB and write to PostgreSQL
            logger.info("Loading education data from MongoDB to PostgreSQL...")
            docs = db_mongo.read_collection("education_data",one=False)
            
            # convert _id from ObjectId to string in ObjectId format
            for doc in docs:
                doc['_id'] = str(doc['_id'])
                
            # convert docs to Spark DataFrame
            education_df = spark.createDataFrame([Row(**doc) for doc in docs])
                
            # log data metrics
            logger.info(f"Education data record count: {education_df.count()}")
            
            # write to PostgreSQL
            db.add_table("education_data", education_df)
            logger.info("Education data successfully loaded into PostgreSQL")
        else:
            logger.warning("No education data was collected, skipping database writes")
            
    except Exception as e:
        logger.error(f"Error processing education data: {str(e)}")
        raise

def main():
    """Main function to orchestrate the data pipeline."""
    try:
        # initialize connections
        spark = initialize_spark()
        print("")
        pg_conn = initialize_postgres()
        print("")
        mongo_conn = initialize_mongodb(spark)
        
        # initialize database helpers
        db = PostgresDBHelper(spark, pg_conn)
        print("")
        db_mongo = MongoDBHelper(spark, mongo_conn)
        print("")
        
        # refresh databases
        logger.info("Refreshing PostgreSQL database...")
        db.refresh_db()
        print("")
        logger.info("Clearing MongoDB database...")
        db_mongo.clear_database()
        print("")
        
        # process data
        process_disability_data(spark, db)
        print("")
        process_income_data(spark, db)
        print("")
        process_health_data(spark, db)
        print("")
        process_education_data(spark, db, db_mongo)
        print("")

        print("Final status of PostgreSQL database:\n")
        db.check_tables().show()
        print("")
        
        # close connections
        pg_conn.close()
        spark.stop()
        print("")
        
        logger.info("Data pipeline completed successfully")
        print("")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
    