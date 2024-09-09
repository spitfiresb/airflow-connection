"""
data_extractor.py
-----------------
This file defines the DataExtractor class, which extracts data from MongoDB and converts ObjectId fields to strings.
"""

from pymongo import MongoClient
from bson import ObjectId


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
            client = MongoClient(
                self.connection_string, serverSelectionTimeoutMS=600000
            )
            db = client[self.database_name]
            collection = db[self.collection_name]
            data = collection.find({})
            extracted_data = []

            for doc in data:
                self._convert_objectid_to_str(doc)
                extracted_data.append(doc)

            print(f"Extracted {len(extracted_data)} documents from MongoDB.")
            return extracted_data

        except Exception as e:
            print(f"Failed to extract data: {str(e)}")
            raise
