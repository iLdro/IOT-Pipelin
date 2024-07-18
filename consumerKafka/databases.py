# databases.py
from private import password
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
import pymongo
import time
from threading import Thread

class MongoDatabases:
    def __init__(self):
        self.uri = "mongodb+srv://public-:" + password + "@iot-project.etft6zm.mongodb.net/?retryWrites=true&w=majority&appName=IOT-Project"
        self.client = MongoClient(self.uri, server_api=ServerApi('1'))
        self.db = self.client['iot_project']

    def insertOne(self, collection, data):
        self.db[collection].insert_one(data)
        print(f"Inserted: {data}")

    def retrieveAll(self, collection):
        documents = self.db[collection].find()
        all_docs = list(documents)
        return all_docs

    def processData(self, collection, ope, column):
        pipeline = [
            {'$match' : {column: {'$ne': None}}},
            {'$group': {'_id': None, ope+column: {'$'+ope : '$'+column}}}
        ]
        result = list(self.db[collection].aggregate(pipeline))
        if result:
            return result[0][ope+column]
        return None

    def percentLogType(self, collection, type):
        count = self.db[collection].count_documents({'log_type' : type})
        total = self.db[collection].count_documents({})
        return count*100/total

    def get_latest_document(self, collection):
        latest_doc = self.db[collection].find_one(sort=[('_id', pymongo.DESCENDING)])
        return latest_doc

    def merge_data(self, device_id):
        # Get latest documents from each collection based on device_id
        thermometer_data = self.db['thermometer_exterior'].find_one({'device_id': device_id}, sort=[('_id', pymongo.DESCENDING)])
        motor_data = self.db['motor'].find_one({'device_id': device_id}, sort=[('_id', pymongo.DESCENDING)])
        general_data = self.db['general'].find_one({'device_id': device_id}, sort=[('_id', pymongo.DESCENDING)])

        if general_data:
            merged_data = {
                'device_id': device_id,
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                'generalStatus': general_data.get('status'),
                'generalSpeed': general_data.get('speed'),
                'fuel_level': general_data.get('fuel_level'),
                'engine_temperature': general_data.get('engine_temperature'),
                'generalLogType': general_data.get('log_type'),
                'motorPower': motor_data.get('power') if motor_data else None,
                'motorSpeed': motor_data.get('speed') if motor_data else None,
                'motorVibration': motor_data.get('vibration') if motor_data else None,
                'motorLog': motor_data.get('log_type'),
                'temperature': thermometer_data.get('temperature') if thermometer_data else None,
                'humidity': thermometer_data.get('humidity') if thermometer_data else None,
                'location': general_data.get('location'),
            }

            # Insert or update the merged data into a new collection or update an existing collection
            # Example: db['merged_data'].insert_one(merged_data)
            self.db['combined'].insert_one(merged_data)
            print(f"Merged data: {merged_data}")
        else:
            print(f"No data found for device_id: {device_id}")


    def computeAndStoreStats(self, collection, stats_collection):
        avg_temp = self.processData(collection, 'avg', 'temperature')
        max_temp = self.processData(collection, 'max', 'temperature')
        min_temp = self.processData(collection, 'min', 'temperature')
        avg_humidity = self.processData(collection, 'avg', 'humidity')
        max_humidity = self.processData(collection, 'max', 'humidity')
        min_humidity = self.processData(collection, 'min', 'humidity')
        warningPercent = self.percentLogType(collection, 'warning')
        errorPercent = self.percentLogType(collection, 'error')

        stats = {
            'average_temperature': avg_temp,
            'max_temperature': max_temp,
            'min_temperature': min_temp,
            'average_humidity': avg_humidity,
            'max_humidity': max_humidity,
            'min_humidity': min_humidity,
            'warning%': warningPercent,
            'error%': errorPercent,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        }
        self.db[stats_collection].insert_one(stats)
        print(f"Stats updated: {stats}")


    def startPeriodicStatsUpdate(self, collection, stats_collection, interval=20):
        def run():
            while True:
                self.computeAndStoreStats(collection, stats_collection)
                time.sleep(interval)

        thread = Thread(target=run)
        thread.daemon = True
        thread.start()

    def startPeriodicMergeUpdate(self, interval=3):
        def run():
            while True:
                lastdoc = db.get_latest_document('general')
                print(lastdoc.get('device_id'))
                db.merge_data(lastdoc.get('device_id'))
                time.sleep(interval)

        thread = Thread(target=run)
        thread.daemon = True
        thread.start()


if __name__ == "__main__":
    db = MongoDatabases()

    db.startPeriodicMergeUpdate()

    # Keep the main thread alive
    while True:
        time.sleep(1)

