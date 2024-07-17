from kafka import KafkaConsumer, KafkaProducer
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

# Kafka consumer configuration
consumer = KafkaConsumer('iot_log', bootstrap_servers='localhost:9092', auto_offset_reset='earliest', enable_auto_commit=True, group_id='my_consumer_group')

# Kafka producer configuration
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

logging.info('Kafka consumer and producer initialized.')

# Iterate over messages from the consumer
for message in consumer:
    try:
        logging.info(f'Consumed message: {message.value}')

        # Parse the JSON message
        data = json.loads(message.value.decode('utf-8'))

        # Extract the device_type from the JSON
        device_type = data.get('device_type')

        # Skip the message if device_type is not present
        if not device_type:
            logging.warning('No device_type found in message, skipping.')
            continue

        # Create a new topic name based on the device_type
        topic_name = device_type

        # Remove the device_type from the JSON
        del data['device_type']

        # Send the filtered message to the corresponding topic
        logging.info(f'Sending message to topic: {topic_name}')
        producer.send(topic_name, data)
        producer.flush()
        consumer.commit()

    except json.JSONDecodeError as e:
        logging.error(f'JSON decode error: {e}')
    except Exception as e:
        logging.error(f'Error processing message: {e}')

# Close the Kafka consumer and producer
consumer.close()
producer.close()
logging.info('Kafka consumer and producer closed.')
