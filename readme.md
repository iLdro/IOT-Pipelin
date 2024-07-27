# Project Overview

This project is a fake data generator for IoT devices, specifically for motors, exterior thermometers, and cars. It simulates the generation of logs for these devices and sends the logs to a Kafka topic named `iot_log`. The logs are then filtered and sent to different Kafka topics based on the device type. Finally, the logs are consumed and loaded into a database, with periodic updates.

## Project Structure

- `fakeDataGenerator/main.py`: Main script to generate logs for all devices and send them to Kafka.
- `consumerKafka/filter.py`: Filters the logs based on device type and sends them to corresponding Kafka topics.
- `consumerKafka/consumer.py`: Consumes the filtered logs and loads them into a database.
- `database/database.py`: Performs periodic updates to the database.

## How to Run the Project

### Prerequisites

1. **Python**: Ensure you have Python installed on your system.
2. **Kafka**: Make sure you have a Kafka server running on `localhost:9092`.
3. **Database**: Ensure you have a database set up and accessible.
4. **Dependencies**: Install the required Python packages.

### Installation

1. Clone the repository:
    ```sh
    git clone <repository_url>
    cd <repository_directory>
    ```

2. Install the required Python packages:
    ```sh
    pip install -r requirements.txt
    ```

### Running the Project

1. **Start your Kafka server** if it is not already running.

2. **Run the main script** to start generating and sending logs:
    ```sh
    python fakeDataGenerator/main.py
    ```

3. **Run the filter script** to filter logs based on device type:
    ```sh
    python consumerKafka/filter.py
    ```

4. **Run the consumer script** to load the filtered logs into the database:
    ```sh
    python consumerKafka/consumer.py
    ```

5. **Run the database script** to perform periodic updates:
    ```sh
    python database/database.py
    ```

## How the Project Works

1. **Log Generation**: The project uses the `Faker` library to generate random data for different types of IoT devices.
2. **Log Types**: Each device can generate three types of logs:
    - **Error**: 10% chance of generating an error log.
    - **Warning**: 10% chance of generating a warning log.
    - **Regular Log**: Generated if neither error nor warning is chosen.
3. **Data Structure**: Each log contains information such as `device_id`, `device_type`, `timestamp`, and other relevant metrics.
4. **Kafka Producer**: The logs are serialized to JSON and sent to the Kafka topic `iot_log` using a Kafka producer.
5. **Log Filtering**: The `filter.py` script consumes logs from the `iot_log` topic, filters them based on `device_type`, and sends them to corresponding Kafka topics.
6. **Database Loading**: The `consumer.py` script consumes the filtered logs from the Kafka topics and loads them into a database.
7. **Periodic Updates**: The `database.py` script performs periodic updates to fetch real-time data about the cars provide and store statistics about the vehicle.