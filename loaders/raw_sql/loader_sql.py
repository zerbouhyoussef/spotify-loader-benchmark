import psycopg2
from kafka import KafkaConsumer
import os
import json
import time

from dotenv import load_dotenv
from benchmark_utils import save_benchmark_result

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)

cursor = conn.cursor()

consumer = KafkaConsumer(
    os.getenv("KAFKA_TOPIC"),
    bootstrap_servers=[os.getenv("KAFKA_BROKER_URL")],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="raw-sql-loader",
    consumer_timeout_ms=30000,
    max_poll_records=100,
    fetch_min_bytes=1,
)

total_expected = int(os.getenv("TOTAL_EXPECTED", "100"))  

start = time.time()
loaded = 0
errors = 0
rows = 0

for message in consumer:
    print(message.value)
    try:
        playlist = json.loads(message.value)
        print(f"Loading playlist: {playlist['pid']}")
        # Insert playlist and get playlist_id
        cursor.execute(
            "INSERT INTO playlist (pid, name, num_tracks, num_holdouts, num_samples) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (pid) DO NOTHING RETURNING playlist_id",
            (playlist['pid'], playlist['name'], playlist['num_tracks'], playlist['num_holdouts'], playlist['num_samples'])
        )
        row = cursor.fetchone()
        if row is None:  # Already existed
            cursor.execute("SELECT playlist_id FROM playlist WHERE pid = %s", (playlist['pid'],))
            row = cursor.fetchone()
        playlist_id = row[0]
        # Do not commit here

        for track in playlist['tracks']:
            # Insert artist and get artist_id
            cursor.execute(
                "INSERT INTO artist (artist_uri, artist_name) VALUES (%s, %s) ON CONFLICT (artist_uri) DO NOTHING RETURNING artist_id",
                (track['artist_uri'], track['artist_name'])
            )
            artist_row = cursor.fetchone()
            if artist_row is None:
                cursor.execute("SELECT artist_id FROM artist WHERE artist_uri = %s", (track['artist_uri'],))
                artist_row = cursor.fetchone()
            artist_id = artist_row[0]

            # Insert album and get album_id
            cursor.execute(
                "INSERT INTO album (album_uri, album_name) VALUES (%s, %s) ON CONFLICT (album_uri) DO NOTHING RETURNING album_id",
                (track['album_uri'], track['album_name'])
            )
            album_row = cursor.fetchone()
            if album_row is None:
                cursor.execute("SELECT album_id FROM album WHERE album_uri = %s", (track['album_uri'],))
                album_row = cursor.fetchone()
            album_id = album_row[0]

            # Insert track and get track_id
            cursor.execute(
                "INSERT INTO track (track_uri, track_name, duration_ms, artist_id, album_id) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (track_uri) DO NOTHING RETURNING track_id",
                (track['track_uri'], track['track_name'], track['duration_ms'], artist_id, album_id)
            )
            track_row = cursor.fetchone()
            if track_row is None:
                cursor.execute("SELECT track_id FROM track WHERE track_uri = %s", (track['track_uri'],))
                track_row = cursor.fetchone()
            track_id = track_row[0]

            # Insert into playlist_track using the real playlist_id and track_id
            cursor.execute(
                "INSERT INTO playlist_track (playlist_id, track_id, pos) VALUES (%s, %s, %s)",
                (playlist_id, track_id, track['pos'])
            )


        conn.commit()  
        print(f"Loaded playlist: {playlist['pid']}")
        loaded += 1
        rows += len(playlist['tracks'])
        if loaded >= total_expected:
            print(f"Reached expected number of playlists ({total_expected}). Stopping consumer loop.")
            break
    except Exception as e:
        print(f"Error loading playlist: {e}")
        conn.rollback()
        errors += 1
        continue

duration = time.time() - start

print("Done loading data!", flush=True)
print(f"Duration: {duration:.2f}s | Playlists: {loaded} | Tracks: {rows} | Errors: {errors}")

save_benchmark_result("raw-sql", duration, loaded, rows, errors)

conn.close()
consumer.close()