from faker import Faker
from kafka import KafkaProducer
import json
import time
import random
from decimal import Decimal
from json import JSONEncoder

# Custom JSON encoder to handle Decimal objects
class DecimalEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)

# Initialize Faker and Kafka producer
fake = Faker()
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v, cls=DecimalEncoder).encode('utf-8')
)

# Define the topic
topic = 'iot-logs'

# Function to generate fake IoT data
def generate_fake_iot_log():
    log_type = 'info'
    status = fake.random_element(elements=('active', 'inactive'))

    # Randomly decide if this log should be an error or warning
    if random.random() < 0.1:  # 10% chance of error
        return {
        'device_id': fake.uuid4(),
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
        'temperature': None,
        'humidity': None,
        'status': status,
        'log_type': 'error',
        'location': {
            'latitude': fake.latitude(),
            'longitude': fake.longitude()
        }
        }
    elif random.random() < 0.1:  # 10% chance of warning
        return{
        'device_id': fake.uuid4(),
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
        'temperature': round(fake.random.uniform(40.0, 70.0), 2) or round(fake.random.uniform(-20.0, 0), 2),
        'humidity': round(fake.random.uniform(70.0, 100.0), 2),
        'status': status,
        'log_type': 'warning',
        'location': {
            'latitude': fake.latitude(),
            'longitude': fake.longitude()
        }
        }
    else :
        return {
        'device_id': fake.uuid4(),
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
        'temperature': round(fake.random.uniform(20.0, 30.0), 2),
        'humidity': round(fake.random.uniform(30.0, 50.0), 2),
        'status': status,
        'log_type': 'log',
        'location': {
            'latitude': fake.latitude(),
            'longitude': fake.longitude()
        }
    }



# Produce fake logs to Kafka topic
try:
    while True:
        iot_log = generate_fake_iot_log()
        producer.send(topic, iot_log)
        print(f"Produced: {iot_log}")
        time.sleep(1)  # Wait for 1 second before sending the next log
except KeyboardInterrupt:
    print("Stopping log generation.")
finally:
    producer.close()
