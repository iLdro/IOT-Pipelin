# databases.py
from private import password
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
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

# test = MongoDatabases()
# print(test.processData('iot_logs', 'avg', 'humidity' ))
# print(test.processData('iot_logs', 'min', 'temperature'))
# print(test.computeAndStoreStats('iot_logs', 'iot_stats'))
# test.startPeriodicStatsUpdate('iot_logs', 'iot_stats')

if __name__ == "__main__":
    test = MongoDatabases()

    test.startPeriodicStatsUpdate('iot_logs', 'iot_stats')

    # Keep the main thread alive
    while True:
        time.sleep(1)

