"""
mongodb_connection.py
----------------------
This file defines the MongoDBConnection class, which is responsible for verifying the MongoDB connection.
"""

from pymongo import MongoClient


class MongoDBConnection:
    def __init__(self, connection_string, server_timeout=5000):
        self.connection_string = connection_string
        self.server_timeout = server_timeout

    def verify_connection(self):
        try:
            client = MongoClient(
                self.connection_string, serverSelectionTimeoutMS=self.server_timeout
            )
            client.admin.command("ping")
            print("MongoDB connection successful.")
        except Exception as e:
            print(f"MongoDB connection failed: {str(e)}")
            raise
