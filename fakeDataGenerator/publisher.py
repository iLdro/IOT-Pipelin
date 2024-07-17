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

def send (topic, message):
    producer.send(topic, message)
    print(f"Produced: {message}")
