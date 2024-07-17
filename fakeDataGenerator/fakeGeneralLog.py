import random
import time
from faker import Faker
from publisher import send
from const import fake_ids

def generate_fake_car_log():
    fake = Faker()

    # Randomly decide the status of the car with a certain probability for 'stopped'
    if random.random() < 0.1:  # 10% chance the car is stopped
        status = 'stopped'
    else:
        status = 'running'

    # If the car is stopped, generate the stopped log data
    if status == 'stopped':
        data = {
            'device_id': fake_ids[random.randint(0, 9)],
            'device_type': 'general',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            'speed': 0,
            'fuel_level': round(fake.random.uniform(5, 15), 2),
            'engine_temperature': 0,
            'status': status,
            'log_type': 'log',
            'location': {
                'latitude': fake.latitude(),
                'longitude': fake.longitude()
            }
        }
    # If the car is running, randomly decide if this log should be an error or warning
    elif random.random() < 0.1:  # 10% chance of error
        data = {
            'device_id': fake_ids[random.randint(0, 9)],
            'device_type': 'general',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            'speed': None,
            'fuel_level': None,
            'engine_temperature': None,
            'status': status,
            'log_type': 'error',
            'location': {
                'latitude': fake.latitude(),
                'longitude': fake.longitude()
            }
        }
    elif random.random() < 0.1:  # 10% chance of warning
        data = {
            'device_id': fake_ids[random.randint(0, 9)],
            'device_type': 'general',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            'speed': round(fake.random.uniform(80, 120), 2),
            'fuel_level': round(fake.random.uniform(5, 15), 2),
            'engine_temperature': round(fake.random.uniform(90, 110), 2),
            'status': status,
            'log_type': 'warning',
            'location': {
                'latitude': fake.latitude(),
                'longitude': fake.longitude()
            }
        }
    else:  # Regular log for running car
        data = {
            'device_id': fake_ids[random.randint(0, 9)],
            'device_type': 'car',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            'speed': round(fake.random.uniform(0, 100), 2),
            'fuel_level': round(fake.random.uniform(20, 80), 2),
            'engine_temperature': round(fake.random.uniform(70, 90), 2),
            'status': status,
            'log_type': 'log',
            'location': {
                'latitude': fake.latitude(),
                'longitude': fake.longitude()
            }
        }
    send('iot_log', data)

# Example usage
generate_fake_car_log()
