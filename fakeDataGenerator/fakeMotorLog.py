import random
import time
from faker import Faker
from publisher import send
from const import fake_ids

def generate_fake_motor_log(status='active'):
    fake = Faker()


    # If the motor is inactive, generate the inactive log data
    if status == 'inactive':
        data = {
            'device_id': fake_ids[random.randint(0, 9)],
            'device_type': 'motor',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            'speed': 0,
            'power': 0,
            'vibration': 0,
            'status': status,
            'log_type': 'log'
        }
    # If the motor is active, randomly decide if this log should be an error or warning
    elif random.random() < 0.1:  # 10% chance of error
        data = {
            'device_id': fake_ids[random.randint(0, 9)],
            'device_type': 'motor',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            'speed': None,
            'power': None,
            'vibration': None,
            'status': status,
            'log_type': 'error'
        }
    elif random.random() < 0.1:  # 10% chance of warning
        data = {
            'device_id': fake_ids[random.randint(0, 9)],
            'device_type': 'motor',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            'speed': round(fake.random.uniform(2000, 5000), 2),
            'power': round(fake.random.uniform(150, 300), 2),
            'vibration': round(fake.random.uniform(10.0, 20.0), 2),
            'status': status,
            'log_type': 'warning'
        }
    else:  # Regular log for active motor
        data = {
            'device_id': fake_ids[random.randint(0, 9)],
            'device_type': 'motor',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            'speed': round(fake.random.uniform(1000, 1500), 2),
            'power': round(fake.random.uniform(100, 150), 2),
            'vibration': round(fake.random.uniform(5.0, 10.0), 2),
            'status': status,
            'log_type': 'log'
        }
    send('iot_log', data)

# Example usage
generate_fake_motor_log()
