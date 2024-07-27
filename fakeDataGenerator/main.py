import time
from faker import Faker
from fakeTempLog import generate_fake_thermotre_exterior_log
from fakeMotorLog import generate_fake_motor_log
from fakeGeneralLog import generate_fake_car_log

fake = Faker()
def generate_fake_log():

    status = fake.random_element(elements=('active', 'inactive'))

    generate_fake_thermotre_exterior_log(status=status)
    time.sleep(0.5)
    generate_fake_car_log(status)
    time.sleep(0.5)
    generate_fake_motor_log(status)
    time.sleep(0.5)
    pass

while True:
    generate_fake_log()
    time.sleep(0.5)