# Ibrahim Malik x23373385

from pyspark.sql.types import StructType, StructField, StringType
from pymongo import MongoClient
from pyspark.sql import SparkSession

# class for assisting in sql db management
class PostgresDBHelper:
    def __init__(self, spark, pg_conn, db_name="disability_db"):
        self.spark = spark
        self.pg_conn = pg_conn
        self.db_name = db_name
        self.jdbc_url = f"jdbc:postgresql://localhost:5432/{self.db_name}"
        self.properties = {
            "user": "ibrahimssmalik",
            "password": "virtualbox2025",
            "driver": "org.postgresql.Driver"
        }

    # main function to access database in PostgreSQL
    def run_query(self, query_string):
        cursor = self.pg_conn.cursor()
        try:
            cursor.execute(query_string)
            if cursor.description:
                column_names = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                schema = StructType([StructField(col, StringType(), True) for col in column_names])
                df = self.spark.createDataFrame(rows, schema=schema)
            else:
                schema = StructType([StructField('empty', StringType(), True)])
                df = self.spark.createDataFrame([], schema=schema)
            self.pg_conn.commit()
        except Exception as e:
            self.pg_conn.rollback()
            print(f"Database error: {e}")
            df = self.spark.createDataFrame([], schema=StructType([]))
        finally:
            cursor.close()
        return df

    # main function to convert query output from Spark Dataframe to Pandas Dataframe
    def run_pandas(self, query_string):
        return self.run_query(query_string).toPandas()

    # main function to select all tables inside database
    def check_tables(self):
        query_string = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
            AND table_schema NOT IN ('information_schema', 'pg_catalog');
        """
        return self.run_query(query_string)

    # main function to delete table from database
    def delete_table(self, table_name):
        try:
            query_check = f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_name = '{table_name}' AND table_type = 'BASE TABLE'
            AND table_schema NOT IN ('information_schema', 'pg_catalog');
            """
            result = self.run_query(query_check)
            if result.count() == 0:
                print(f"Error: Table '{table_name}' does not exist.")
            else:
                self.run_query(f"DROP TABLE {table_name};")
                print(f"Table '{table_name}' successfully deleted.")
        except Exception as e:
            print(f"An error occurred: {e}")

    # main function to delete all tables from database
    def refresh_db(self):
        tables_df = self.check_tables()
        table_list = [row[0] for row in tables_df.select(tables_df.columns[0]).collect()]
        if not table_list:
            print("Database is already empty. No tables to delete.")
            return
        for table in table_list:
            self.delete_table(table)

    # main function to write table to database
    def add_table(self, table_name, table_df):
        tables_df = self.check_tables()
        existing_tables = [row[0] for row in tables_df.select(tables_df.columns[0]).collect()]
        if table_name in existing_tables:
            print('Table already exists. Skipping write.')
        else:
            table_df.write.jdbc(
                url=self.jdbc_url,
                table=table_name,
                mode="overwrite",
                properties=self.properties
            )
            print(f"{table_name} table written to PostgreSQL.")

# class for assisting in nosql db management
class MongoDBHelper:
    def __init__(self, spark, mongo_conn, db_name="education_db"):
        self.spark = spark
        self.mongo_conn = mongo_conn
        self.db_name = db_name
        self.client = MongoClient(mongo_conn)
        self.db = self.client[db_name]

    # list all collections in database
    def list_collections(self):
        return self.db.list_collection_names()

    # read a collection and return its documents
    def read_collection(self, collection_name,one=True):
        try:
            collection = self.db[collection_name] # access the MongoDB collection
            if one:
                documents = collection.find_one() # fetch one document
                documents = [documents] if documents else [] # wrap in a list for consistency
            else:
                documents = list(collection.find()) # fetch all documents
            print(f"Fetched {len(documents)} records from collection '{collection_name}'.")
            return documents # return the list of documents
        except Exception as e:
            print(f"An error occurred while reading from MongoDB: {e}")
            return []

    # write list of documents to a collection
    def write_collection(self, collection_name, documents):
        try:
            # ensure collection exists and insert data
            collection = self.db[collection_name]
            if documents:
                collection.insert_many(documents)
                print(f"Inserted {len(documents)} records into collection '{collection_name}'.")
            else:
                print(f"No data to insert into collection '{collection_name}'.")
        except Exception as e:
            print(f"An error occurred while writing to MongoDB: {e}")

    # drop a collection
    def drop_collection(self, collection_name):
        if collection_name in self.list_collections():
            self.db.drop_collection(collection_name)
            print(f"Collection '{collection_name}' dropped.")
        else:
            print(f"Collection '{collection_name}' does not exist.")

    # drop all collections in database
    def clear_database(self):
        collections = self.list_collections()
        if not collections:
            print("Database is already empty. No tables to delete.")
        else:
            for col in collections:
                self.drop_collection(col)
