import psycopg2
from kafka import KafkaConsumer
import os 
import json

from dotenv import load_dotenv

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
)

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
    except Exception as e:
        print(f"Error loading playlist: {e}")
        continue

cursor.execute("TRUNCATE playlist_track, track, playlist, album, artist RESTART IDENTITY CASCADE;")
conn.commit()

print("Done loading data!", flush=True)

conn.close()
consumer.close()