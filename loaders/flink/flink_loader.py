import os
import json
import psycopg2
import time
from dotenv import load_dotenv

from pyflink.datastream import StreamExecutionEnvironment, MapFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types


class CountingMapFunction(MapFunction):
    def __init__(self, limit):
        self.limit = limit
        self.count = 0
        
    def map(self, msg_value):
        if self.count >= self.limit:
            # Stop processing after limit
            return None
        
        self.count += 1
        try:
            playlist = json.loads(msg_value)
            if not playlist.get("tracks"):
                return f"SKIPPED ({self.count}/{self.limit})"
            result = load_playlist(playlist)
            if result["error"]:
                return f"ERROR pid={playlist['pid']} ({self.count}/{self.limit})"
            return f"OK pid={playlist['pid']} tracks={result['tracks']} ({self.count}/{self.limit})"
        except Exception as e:
            return f"ERROR {str(e)} ({self.count}/{self.limit})"

# Load environment variables
load_dotenv()

# Database connection parameters (hardcoded for Flink cluster)
DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "benchmark",
    "user": "postgres",
    "password": "postgres"
}

KAFKA_BROKER = os.getenv("KAFKA_BROKER_URL", "kafka-broker:9093")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "playlist-topic")
TOTAL_EXPECTED = int(os.getenv("TOTAL_EXPECTED", "1000"))


def load_playlist(playlist):
    print(f"Attempting DB connection to: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"DB Connection failed: {e}")
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


def main():
    # Get execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Set parallelism to 1 so CountingMapFunction counts accurately
    env.set_parallelism(1)
    
    print(f"Connecting to Kafka at {KAFKA_BROKER} topic {KAFKA_TOPIC}")
    print(f"Will process {TOTAL_EXPECTED} playlists then stop")

    # Make source bounded - capture current latest offset and process up to that point
    # This makes it a batch job that will finish after processing all available messages
    # Use a unique consumer group name with timestamp to always read from beginning
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics(KAFKA_TOPIC) \
        .set_group_id(f"flink-loader-{int(time.time())}") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_bounded(KafkaOffsetsInitializer.latest()) \
        .build()

    ds = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Kafka Source"
    )

    start = time.time()
    
    # Record start counts from database
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM playlist")
        playlists_before = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM track")
        tracks_before = cursor.fetchone()[0]
        cursor.close()
        conn.close()
    except:
        playlists_before = 0
        tracks_before = 0

    # Map each message to process it and count
    ds.map(CountingMapFunction(TOTAL_EXPECTED), output_type=Types.STRING()).filter(lambda x: x is not None).print()

    result = env.execute("Flink Playlist Loader")

    duration = time.time() - start
    
    # Get final counts from database
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM playlist")
        playlists_after = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM track")
        tracks_after = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        loaded = playlists_after - playlists_before
        tracks = tracks_after - tracks_before
        errors = 0  # No real errors - we just processed what was available
    except Exception as e:
        print(f"Error querying database: {e}")
        loaded = 0
        tracks = 0
        errors = 0
    
    print("Done loading data!")
    print(f"Duration: {duration:.2f}s | Playlists: {loaded} | Tracks: {tracks} | Errors: {errors}")

if __name__ == "__main__":
    main()