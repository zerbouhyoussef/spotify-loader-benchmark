from kafka import KafkaProducer
import json
import os

broker_url = os.getenv('KAFKA_BROKER_URL', 'kafka-broker:9093')
producer = KafkaProducer(bootstrap_servers=broker_url)

data_path = "/app/challenge_set.json"
with open(data_path, 'r') as f:
    data = json.load(f)

print(len(data))