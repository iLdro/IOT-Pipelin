import json
import time
import threading
from kafka import KafkaConsumer
from databases import MongoDatabases  # Replace with your actual MongoDB class implementation

class Consumer(threading.Thread):
    def __init__(self, topic_name):
        super().__init__()
        self.topic_name = topic_name
        self.daemon = True  # Threads will exit when the main program exits
        self.database = MongoDatabases()  # Initialize MongoDB connection
        self.kafka_consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my_consumer_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def run(self):
        print(f"Starting consumer for topic: {self.topic_name}")
        try:
            for message in self.kafka_consumer:
                message_data = message.value
                print(f"Consumed from {self.topic_name}: {message_data}")
                time.sleep(1)  # Simulate processing time

                # Insert message into MongoDB
                self.database.insertOne(collection=self.topic_name, data=message_data)  # Assuming insertOne method exists in your MongoDB class

                # Commit the offset manually after processing
                self.kafka_consumer.commit()

        except KeyboardInterrupt:
            print(f"Stopping consumer for topic: {self.topic_name}")
        finally:
            self.kafka_consumer.close()
            print(f"Consumer stopped for topic: {self.topic_name}")

# List of topics to consume from
topics = ["thermometer_exterior", "general", "motor"]

# Create and start a thread for each topic
threads = []
for topic_name in topics:
    thread = Consumer(topic_name)
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()

print("All consumers stopped.")
