import os
import json
import psycopg2
import time

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy

# Database connection parameters (hardcoded for Flink cluster)
DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "benchmark",
    "user": "postgres",
    "password": "postgres"
}

KAFKA_BROKER = "kafka-broker:9093"
KAFKA_TOPIC = "playlist-topic"


def load_playlist(playlist):
    print(f"Attempting DB connection to: {DB_CONFIG['host']}:{DB_CONFIG['port']}", flush=True)
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"DB Connection failed: {e}", flush=True)
        raise
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO playlist (pid, name, num_tracks, num_holdouts, num_samples) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (pid) DO NOTHING RETURNING playlist_id",
            (
                playlist['pid'],
                playlist.get('name'),
                playlist.get('num_tracks'),
                playlist.get('num_holdouts'),
                playlist.get('num_samples')
            ),
        )
        row = cursor.fetchone()
        if row is None:
            cursor.execute("SELECT playlist_id FROM playlist WHERE pid = %s", (playlist['pid'],))
            row = cursor.fetchone()
        playlist_id = row[0]

        for track in playlist['tracks']:
            # Insert artist and get artist_id
            cursor.execute(
                "INSERT INTO artist (artist_uri, artist_name) VALUES (%s, %s) "
                "ON CONFLICT (artist_uri) DO NOTHING RETURNING artist_id",
                (track['artist_uri'], track['artist_name'])
            )
            artist_row = cursor.fetchone()
            if artist_row is None:
                cursor.execute("SELECT artist_id FROM artist WHERE artist_uri = %s", (track['artist_uri'],))
                artist_row = cursor.fetchone()
            artist_id = artist_row[0]

            # Insert album and get album_id
            cursor.execute(
                "INSERT INTO album (album_uri, album_name) VALUES (%s, %s) "
                "ON CONFLICT (album_uri) DO NOTHING RETURNING album_id",
                (track['album_uri'], track['album_name'])
            )
            album_row = cursor.fetchone()
            if album_row is None:
                cursor.execute("SELECT album_id FROM album WHERE album_uri = %s", (track['album_uri'],))
                album_row = cursor.fetchone()
            album_id = album_row[0]

            # Insert track and get track_id
            cursor.execute(
                "INSERT INTO track (track_uri, track_name, duration_ms, artist_id, album_id) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (track_uri) DO NOTHING RETURNING track_id",
                (track['track_uri'], track['track_name'], track['duration_ms'], artist_id, album_id)
            )
            track_row = cursor.fetchone()
            if track_row is None:
                cursor.execute("SELECT track_id FROM track WHERE track_uri = %s", (track['track_uri'],))
                track_row = cursor.fetchone()
            track_id = track_row[0]

            # Insert into playlist_track
            cursor.execute(
                "INSERT INTO playlist_track (playlist_id, track_id, pos) "
                "VALUES (%s, %s, %s) "
                "ON CONFLICT (playlist_id, track_id) DO NOTHING",
                (playlist_id, track_id, track['pos'])
            )

        conn.commit()
        return {"tracks": len(playlist['tracks']), "error": False}
    except Exception as e:
        print(f"Error loading playlist: {e}")
        conn.rollback()
        return {"tracks": 0, "error": True}
    finally:
        cursor.close()
        conn.close()

def playlist_message_to_db(msg_value):
    try:
        playlist = json.loads(msg_value)
        if not playlist.get("tracks"):
            return "SKIPPED"
        result = load_playlist(playlist)
        if result["error"]:
            return f"ERROR pid={playlist['pid']}"
        return f"OK pid={playlist['pid']} tracks={result['tracks']}"
    except Exception as e:
        return f"ERROR {str(e)}"

def main():
    # Get execution environment (will run in embedded mode)
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Configure parallelism
    env.set_parallelism(2)
    
    print(f"Connecting to Kafka at {KAFKA_BROKER} topic {KAFKA_TOPIC}", flush=True)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics(KAFKA_TOPIC) \
        .set_group_id("flink-loader") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Kafka Source"
    )

    start = time.time()

    ds.map(lambda msg: playlist_message_to_db(msg)).print()

    env.execute("Flink Playlist Loader")

    duration = time.time() - start
    print(f"Duration: {duration:.2f}s")

if __name__ == "__main__":
    main()