import time
from fakeTempLog import generate_fake_thermotre_exterior_log
from fakeMotorLog import generate_fake_motor_log
from fakeGeneralLog import generate_fake_car_log

def generate_fake_log():
    generate_fake_thermotre_exterior_log()
    time.sleep(0.5)
    generate_fake_car_log()
    time.sleep(0.5)
    generate_fake_motor_log()
    time.sleep(0.5)
    pass

while True:
    generate_fake_log()
    time.sleep(0.5)