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
                'outdoorLog' : thermometer_data.get('log_type'),
                'location': general_data.get('location'),
            }

            # Insert or update the merged data into a new collection or update an existing collection
            # Example: db['merged_data'].insert_one(merged_data)
            self.db['combined'].insert_one(merged_data)
            print(f"Merged data: {merged_data}")
        else:
            print(f"No data found for device_id: {device_id}")

    def computeVehicleStats(self, device_id):
        pipeline = [
            {
                '$match': {
                    'device_id': device_id
                }
            },
            {
                '$group': {
                    '_id': '$device_id',
                    'averageGeneralSpeed': {'$avg': '$generalSpeed'},
                    'maxGeneralSpeed': {'$max': '$generalSpeed'},
                    'averageMotorSpeed': {'$avg': '$motorSpeed'},
                    'maxMotorSpeed': {'$max': '$motorSpeed'},
                    'averageEngineTemperature': {'$avg': '$engine_temperature'},
                    'maxEngineTemperature': {'$max': '$engine_temperature'},
                    'count': {'$sum': 1}
                }
            }
        ]
        results = self.db['combined'].aggregate(pipeline)
        for result in results:
            average_data = {
                'device_id': device_id,
                'averageGeneralSpeed': result['averageGeneralSpeed'],
                'maxGeneralSpeed': result['maxGeneralSpeed'],
                'averageMotorSpeed': result['averageMotorSpeed'],
                'maxMotorSpeed': result['maxMotorSpeed'],
                'averageEngineTemperature': result['averageEngineTemperature'],
                'maxEngineTemperature': result['maxEngineTemperature'],
                'nbLogs': result['count'],
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            }
            self.db['VehiculeStats'].update_one(
                {'device_id': device_id},
                {'$set': average_data},
                upsert=True
            )
            print(f"Updated stats for device ID: {result['_id']}, Data: {average_data}")


    def computeOutdoorStats(self, device_id):
        pipeline = [
            {
                '$match': {
                    'device_id': device_id
                }
            },
            {
                '$group': {
                    '_id': '$device_id',
                    'averageTemperature': {'$avg': '$temperature'},
                    'maxTemperature' : {'$max': '$temperature'},
                    'minTemperature' : {'$min': '$temperature'},
                    'averageHumidity': {'$avg': '$humidity'},
                    'maxHumidity': {'$max': '$humidity'},
                    'minHumidity': {'$min': '$humidity'},
                    'count': {'$sum': 1}

                }
            }
        ]
        results = self.db['combined'].aggregate(pipeline)
        for result in results:
            average_data = {
                'averageOutdoorTemperature': result['averageTemperature'],
                'maxOutdoorTemperature': result['maxTemperature'],
                'minOutdoorTemperature': result['minTemperature'],
                'averageOutdoorHumidity': result['averageHumidity'],
                'maxOutdoorHumidity': result['maxHumidity'],
                'minOutdoorHumidity': result['minHumidity'],
                'count': result['count'],
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            }
            self.db['OutdoorStats'].update_one(
                {'device_id': device_id},
                {'$set': average_data},
                upsert=True
            )
            print(f"Updated stats for device ID: {result['_id']}, Data: {average_data}")

    def process_all_devices(self):
        # Retrieve all unique device IDs from the combined collection
        device_ids = self.db['combined'].distinct('device_id')

        for device_id in device_ids:
            self.computeVehicleStats(device_id)
            self.computeOutdoorStats(device_id)
            print(f"Processed device_id: {device_id}")

    def find_device_with_most_errors(self):
        pipeline = [
            {
                '$match': {
                    '$or': [
                        {'generalLogType': 'error'},
                        {'motorLog': 'error'},
                        {'outdoorLog': 'error'}
                    ]
                }
            },
            {
                '$group': {
                    '_id': '$device_id',
                    'errorCount': {'$sum': 1}
                }
            },
            {
                '$sort': {'errorCount': -1}
            },
            {
                '$limit': 1
            }
        ]

        results = list(self.db['combined'].aggregate(pipeline))

        if results:
            most_errors = results[0]
            error_data = {
                'device_id': most_errors['_id'],
                'errorCount': most_errors['errorCount'],
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            }

            self.db['error_stats'].update_one(
                {'device_id': most_errors['_id']},
                {'$set': error_data},
                upsert=True
            )

            print(f"Device with most errors: {error_data}")
        else:
            print("No errors found in the combined collection.")



    def count_log_types(self):
        # Get all distinct device_ids
        distinct_device_ids = self.db['combined'].distinct('device_id')

        for device_id in distinct_device_ids:
            pipeline = [
                {
                    '$match': {
                        'device_id': device_id
                    }
                },
                {
                    '$group': {
                        '_id': {
                            'generalLogType': '$generalLogType',
                            'motorLog': '$motorLog',
                            'outdoorLog': '$outdoorLog'
                        },
                        'count': {'$sum': 1}
                    }
                }
            ]
            results = self.db['combined'].aggregate(pipeline)
            log_counts = {'error': 0, 'warning': 0, 'log': 0}

            for result in results:
                for log_type, count in result['_id'].items():
                    if log_type in log_counts:
                        log_counts[log_type] += result['count']

            # Prepare data to be stored in 'logsStats'
            stats_data = {
                'device_id': device_id,
                'error_count': log_counts['error'],
                'warning_count': log_counts['warning'],
                'log_count': log_counts['log'],
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            }

            # Insert or update the stats data
            self.db['logsStats'].update_one(
                {'device_id': device_id},
                {'$set': stats_data},
                upsert=True
            )
            print(f"Updated log counts for device ID: {device_id}, Data: {stats_data}")


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

    def startPeriodicStatsUpdate(self, interval=10):
        def run():
            while True:
                db.process_all_devices()
                db.count_log_types()
                db.find_device_with_most_errors()
                time.sleep(interval)

        thread = Thread(target=run)
        thread.daemon = True
        thread.start()


if __name__ == "__main__":
    db = MongoDatabases()

    db.startPeriodicMergeUpdate()
    db.startPeriodicStatsUpdate()

    # Keep the main thread alive
    while True:
        time.sleep(1)

