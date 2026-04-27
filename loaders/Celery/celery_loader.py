import os
import time
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv
from tasks import load_playlist
from benchmark_utils import save_benchmark_result

load_dotenv()

total_expected = int(os.getenv("TOTAL_EXPECTED", "100"))

consumer = KafkaConsumer(
    os.getenv("KAFKA_TOPIC"),
    bootstrap_servers=[os.getenv("KAFKA_BROKER_URL")],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="celery-loader",
    consumer_timeout_ms=30000,
    max_poll_records=100,
    fetch_min_bytes=1,
)

start = time.time()
loaded = 0
errors = 0
rows = 0

futures = []

for message in consumer:
    try:
        playlist = json.loads(message.value)
        if not playlist.get("tracks"):
            continue
        futures.append(load_playlist.delay(playlist))
        if len(futures) >= total_expected:
            break
    except Exception as e:
        print(f"Error loading playlist: {e}")
        errors += 1
        continue

# Wait for all tasks to complete and count stats
for future in futures:
    try:
        result = future.get(timeout=120)  # 120 seconds per playlist
        if result["error"]:
            errors += 1
        else:
            loaded += 1
            rows += result["tracks"]
    except Exception as e:
        print(f"Error waiting for celery result: {e}")
        errors += 1

consumer.close()
duration = time.time() - start

print("Done loading data!", flush=True)
print(f"Duration: {duration:.2f}s | Playlists: {loaded} | Tracks: {rows} | Errors: {errors}")

save_benchmark_result("celery", duration, loaded, rows, errors)