"""
dag_mongo_extraction.py
-----------------------
This is the main DAG file. It orchestrates the tasks of verifying MongoDB connection, extracting data, 
and processing data in sequence using Airflow.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from mongodb_connection import MongoDBConnection
from data_extractor import DataExtractor
from data_processor import DataProcessor

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 9, 1),
    "end_date": datetime(2024, 9, 30),
    "retries": 0,
}

# Define the DAG
with DAG(
    "mongo_connection_and_extraction_dag",
    default_args=default_args,
    description="A DAG to verify MongoDB connection and then extract and process data.",
    schedule_interval=None,
    catchup=False,
) as dag:

    connection_string = "connection_string"
    mongo_connection = MongoDBConnection(connection_string)
    data_extractor = DataExtractor(
        connection_string, "database_name", "collection_name"
    )
    data_processor = DataProcessor()

    verify_connection_task = PythonOperator(
        task_id="verify_mongo_connection",
        python_callable=mongo_connection.verify_connection,
    )

    extract_data_task = PythonOperator(
        task_id="extract_data_from_mongo",
        python_callable=data_extractor.extract_data,
        execution_timeout=timedelta(minutes=1),
    )

    process_data_task = PythonOperator(
        task_id="process_data",
        python_callable=data_processor.process_data,
        provide_context=True,
    )

    verify_connection_task >> extract_data_task >> process_data_task
