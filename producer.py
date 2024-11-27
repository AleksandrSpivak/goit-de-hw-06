# Генерить поток даних та пише їх в AS_topic_in

from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'AS_topic_in'

sensor_id = random.randint(1000, 9999)

print(f"Starting simulation for sensor ID: {sensor_id}")

try:
    while True:
        data = {
            "sensor_id": sensor_id,      
            "timestamp": time.time(),
            "temperature": random.uniform(10, 30), 
            "humidity": random.uniform(65, 85)
        }

        producer.send(topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush() 
        print(f"Data sent to topic '{topic_name}': {data}")
        time.sleep(1) 
except KeyboardInterrupt:
    print("\nSimulation stopped by user.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    producer.close() 
    print("Kafka producer closed.")


