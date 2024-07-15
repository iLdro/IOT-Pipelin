# databases.py
from private import password
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

class MongoDatabases:
    def __init__(self):
        self.uri = "mongodb+srv://public-:" + password + "@iot-project.etft6zm.mongodb.net/?retryWrites=true&w=majority&appName=IOT-Project"
        self.client = MongoClient(self.uri, server_api=ServerApi('1'))
        self.db = self.client['iot_project']

    def insertOne(self, collection, data):
        self.db[collection].insert_one(data)
        print(f"Inserted: {data}")
