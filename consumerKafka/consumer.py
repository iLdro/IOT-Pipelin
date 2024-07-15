import json
import time
from kafka import KafkaConsumer
from databases import MongoDatabases  # Assuming this is your custom MongoDB class

class Consumer:
    def __init__(self):
        # Initialize MongoDB connection
        self.database = MongoDatabases()
        # Initialize Kafka consumer with a group_id
        self.kafka_consumer = KafkaConsumer(
            'iot-logs',  # Kafka topic to consume from
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',  # Start consuming from the earliest offset
            enable_auto_commit=False,  # Disable auto commit offsets
            group_id='my_consumer_group',  # Specify a group_id
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def consume(self):
        try:
            for message in self.kafka_consumer:
                message_data = message.value
                print(f"Consumed: {message_data}")
                time.sleep(1)  # Simulate processing time

                # Insert message into MongoDB
                self.database.insertOne('iot_logs', message_data)

                # Commit the offset manually after processing
                self.kafka_consumer.commit()

        except KeyboardInterrupt:
            print("Stopping consumer.")
        finally:
            self.kafka_consumer.close()
            print("Consumer stopped.")

# Instantiate the Consumer class and start consuming
consumer = Consumer()
consumer.consume()