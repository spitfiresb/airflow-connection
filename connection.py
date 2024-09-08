# Airflow DAG for MongoDB Data Extraction, Processing, and Tabulation
# 
# This DAG demonstrates a generic workflow to:
# 1. Verify the connection to a MongoDB instance.
# 2. Extract data from a specified MongoDB database and collection.
# 3. Process and tabulate the extracted data using pandas.
# 
# The script defines the following classes:
# - MongoDBConnection: Responsible for verifying the MongoDB connection.
# - DataExtractor: Handles the extraction of data from MongoDB, including 
#   the conversion of MongoDB ObjectIds to strings.
# - DataProcessor: Processes the extracted data and performs checks on 
#   the tabulated DataFrame (e.g., length verification, column presence, 
#   and null value detection).
# 
# This DAG is generalized to allow dynamic input of MongoDB connection 
# strings, database names, and collection names, making it reusable for 
# different datasets.
# 
# The Airflow DAG consists of three tasks:
# - verify_mongo_connection: Verifies the connection to the MongoDB instance.
# - extract_data_from_mongo: Extracts data from the MongoDB collection.
# - process_data: Processes the extracted data and checks for consistency.

from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
from datetime import datetime, timedelta
from bson import ObjectId
import pandas as pd


class MongoDBConnection:
    def __init__(self, connection_string, server_timeout=5000):
        self.connection_string = connection_string
        self.server_timeout = server_timeout

    def verify_connection(self):
        try:
            client = MongoClient(self.connection_string, serverSelectionTimeoutMS=self.server_timeout)
            client.admin.command('ping')
            print("MongoDB connection successful.")
        except Exception as e:
            print(f"MongoDB connection failed: {str(e)}")
            raise


class DataExtractor:
    def __init__(self, connection_string, database_name, collection_name):
        self.connection_string = connection_string
        self.database_name = database_name
        self.collection_name = collection_name

    def _convert_objectid_to_str(self, data):
        """Recursively convert ObjectId to string in nested dictionaries and lists."""
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, ObjectId):
                    data[key] = str(value)
                elif isinstance(value, dict):
                    self._convert_objectid_to_str(value)
                elif isinstance(value, list):
                    for i in range(len(value)):
                        if isinstance(value[i], ObjectId):
                            value[i] = str(value[i])
                        elif isinstance(value[i], dict):
                            self._convert_objectid_to_str(value[i])
        elif isinstance(data, list):
            for i in range(len(data)):
                if isinstance(data[i], ObjectId):
                    data[i] = str(data[i])
                elif isinstance(data[i], dict):
                    self._convert_objectid_to_str(data[i])

    def extract_data(self, **kwargs):
        try:
            client = MongoClient(self.connection_string, serverSelectionTimeoutMS=60000)  # 60 second timeout
            db = client[self.database_name]
            collection = db[self.collection_name]
            
            data = collection.find({})
            extracted_data = []
            
            # Convert ObjectId fields to strings in each document
            for doc in data:
                self._convert_objectid_to_str(doc)
                extracted_data.append(doc)
            
            print(f"Extracted {len(extracted_data)} documents from MongoDB.")
            return extracted_data
            
        except Exception as e:
            print(f"Failed to extract data: {str(e)}")
            raise


class DataProcessor:
    @staticmethod
    def process_data(**kwargs):
        ti = kwargs['ti']
        extracted_data = ti.xcom_pull(task_ids='extract_data_from_mongo')
        
        if extracted_data:
            print(f"First document: {extracted_data[0]}")
        
        # Tabulating
        df = pd.DataFrame(extracted_data)
        print(df.head())  # This prints the first few rows of the DataFrame
        
        # Check 1: Verify the length of the DataFrame
        expected_length = len(extracted_data)
        actual_length = len(df)
        if actual_length == expected_length:
            print(f"Length check passed: {actual_length} rows.")
        else:
            print(f"Length check failed: expected {expected_length}, but got {actual_length}.")
        
        # Check 2: Ensure all expected columns are present
        expected_columns = ['_id', 'customerId', 'topicId', 'cimEvent', '_class']
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if not missing_columns:
            print("All expected columns are present.")
        else:
            print(f"Missing columns: {missing_columns}")
        
        # Check 3: Identify null or missing values in the DataFrame
        null_values = df.isnull().sum()
        if null_values.any():
            print("Warning: There are null values in the DataFrame.")
            print(null_values[null_values > 0])
        else:
            print("No null values found in the DataFrame.")


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 1),
    'end_date': datetime(2024, 9, 30),
    'retries': 0,
}

# Define the DAG
with DAG(
    'mongo_connection_and_extraction_dag',
    default_args=default_args,
    description='A DAG to verify MongoDB connection and then extract and process data.',
    schedule_interval=None,  # Trigger manually
    catchup=False,
) as dag:
    
    # Create instances of the classes
    connection_string = "Your_connection_string"
    mongo_connection = MongoDBConnection(connection_string)
    data_extractor = DataExtractor(connection_string, "Database_name", "Collection_name")
    data_processor = DataProcessor()

    # Task 1: Verify MongoDB Connection
    verify_connection_task = PythonOperator(
        task_id='verify_mongo_connection',
        python_callable=mongo_connection.verify_connection,
    )
    
    # Task 2: Extract Data from MongoDB
    extract_data_task = PythonOperator(
        task_id='extract_data_from_mongo',
        python_callable=data_extractor.extract_data,
        execution_timeout=timedelta(minutes=1),
    )
    
    # Task 3: Process Data
    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=data_processor.process_data,
        provide_context=True,
    )

    verify_connection_task >> extract_data_task >> process_data_task