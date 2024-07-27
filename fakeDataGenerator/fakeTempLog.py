import random
import time
from faker import Faker
from publisher import send
from const import fake_ids


def generate_fake_thermotre_exterior_log(status):
    fake = Faker()

    # Randomly decide if this log should be an error or warning
    if random.random() < 0.1:  # 10% chance of error
        data = {
        'device_id': fake_ids[random.randint(0, 9)],
        'device_type': 'thermometer_exterior',
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
        'temperature': None,
        'humidity': None,
        'status': status,
        'log_type': 'error'
        }
    elif random.random() < 0.1:  # 10% chance of warning
        data = {
        'device_id': fake_ids[random.randint(0, 9)],
        'device_type': 'thermometer_exterior',
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
        'temperature': round(fake.random.uniform(40.0, 70.0), 2) or round(fake.random.uniform(-20.0, 0), 2),
        'humidity': round(fake.random.uniform(70.0, 100.0), 2),
        'status': status,
        'log_type': 'warning'
        }
    else :
        data = {
        'device_id': fake_ids[random.randint(0, 9)],
        'device_type': 'thermometer_exterior',
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
        'temperature': round(fake.random.uniform(20.0, 30.0), 2),
        'humidity': round(fake.random.uniform(30.0, 50.0), 2),
        'status': status,
        'log_type': 'log'
    }
    send('iot_log', data)

