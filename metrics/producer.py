from kafka import KafkaProducer
import json
import os
import time
import sys

print("Starting producer...", flush=True)

broker_url = os.getenv('KAFKA_BROKER_URL', 'kafka-broker:9093')
print(f"Connecting to Kafka at {broker_url}...", flush=True)

try:
    producer = KafkaProducer(bootstrap_servers=broker_url)
    print("Connected to Kafka successfully!", flush=True)
except Exception as e:
    print(f"Failed to connect to Kafka: {e}", flush=True)
    sys.exit(1)

TOPIC_NAME = "playlist-topic"
data_path = "/app/challenge_set.json"

print(f"Loading data from {data_path}...", flush=True)

try:
    with open(data_path, "r") as infile:
        json_data = json.load(infile)
        list_data = json_data["playlists"] if isinstance(json_data, dict) else json_data
        
        print(f"Loaded {len(list_data)} playlists. Starting to send...", flush=True)
        valid = [p for p in list_data if p["tracks"] != []][:100000]  # Send 100k playlists
        for idx, playlist in enumerate(valid):
            producer.send(TOPIC_NAME, json.dumps(playlist).encode('utf-8'))
            if idx % 1000 == 0:  # Print every 1000th message to reduce log spam
                print(f"Sent {idx}/{len(valid)} playlists...", flush=True)
        
        producer.flush()  # Ensure all messages are sent
        print(f"Finished sending {len(valid)} playlists!", flush=True)
        
except FileNotFoundError as e:
    print(f"File not found: {e}", flush=True)
    sys.exit(1)
except json.JSONDecodeError as e:
    print(f"Failed to decode JSON: {e}", flush=True)
    sys.exit(1)
except Exception as e:
    print(f"Unexpected error: {e}", flush=True)
    sys.exit(1)
finally:
    producer.close()
    print("Producer closed.", flush=True)