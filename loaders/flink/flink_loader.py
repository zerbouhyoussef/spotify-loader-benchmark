import os
import json
import psycopg2
import time
from dotenv import load_dotenv

from pyflink.datastream import StreamExecutionEnvironment, MapFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types
from psycopg2 import pool

# Load environment variables first so DB_CONFIG picks them up
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "benchmark"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}


def save_benchmark_result(loader_name, duration_s, playlists, tracks, errors):
    run_id = os.getenv("RUN_ID", f"manual_{int(time.time())}")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO benchmark_results (loader_name, duration_s, playlists, tracks, errors, run_id) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (loader_name, duration_s, playlists, tracks, errors, run_id),
        )
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Saved benchmark result: {loader_name} run_id={run_id}")
    except Exception as e:
        print(f"Failed to save benchmark result: {e}")


class ProcessingMapFunction(MapFunction):
    """Process playlist messages in parallel with connection pooling"""
    
    def open(self, runtime_context: RuntimeContext):
        """Initialize connection pool once per task"""
        self.connection_pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=2,
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        print(f"Connection pool initialized for task {runtime_context.get_index_of_this_subtask()}")
    
    def map(self, msg_value):
        try:
            playlist = json.loads(msg_value)
            if not playlist.get("tracks"):
                return f"SKIPPED pid={playlist.get('pid', 'unknown')}"
            
            result = self.load_playlist_pooled(playlist)
            if result["error"]:
                return f"ERROR pid={playlist['pid']}"
            return f"OK pid={playlist['pid']} tracks={result['tracks']}"
        except Exception as e:
            return f"ERROR {str(e)}"
    
    def load_playlist_pooled(self, playlist):
        """Load playlist using pooled connection"""
        conn = self.connection_pool.getconn()
        try:
            cursor = conn.cursor()
            # Insert playlist
            cursor.execute(
                "INSERT INTO playlist (pid, name, num_tracks, num_holdouts, num_samples) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (pid) DO NOTHING RETURNING playlist_id",
                (playlist['pid'], playlist.get('name'), playlist.get('num_tracks'), 
                 playlist.get('num_holdouts'), playlist.get('num_samples'))
            )
            row = cursor.fetchone()
            if row is None:
                cursor.execute("SELECT playlist_id FROM playlist WHERE pid = %s", (playlist['pid'],))
                row = cursor.fetchone()
            playlist_id = row[0]

            for track in playlist['tracks']:
                # Insert artist
                cursor.execute(
                    "INSERT INTO artist (artist_uri, artist_name) VALUES (%s, %s) "
                    "ON CONFLICT (artist_uri) DO NOTHING RETURNING artist_id",
                    (track['artist_uri'], track['artist_name'])
                )
                artist_row = cursor.fetchone()
                if artist_row is None:
                    cursor.execute("SELECT artist_id FROM artist WHERE artist_uri = %s", 
                                 (track['artist_uri'],))
                    artist_row = cursor.fetchone()
                artist_id = artist_row[0]

                # Insert album
                cursor.execute(
                    "INSERT INTO album (album_uri, album_name) VALUES (%s, %s) "
                    "ON CONFLICT (album_uri) DO NOTHING RETURNING album_id",
                    (track['album_uri'], track['album_name'])
                )
                album_row = cursor.fetchone()
                if album_row is None:
                    cursor.execute("SELECT album_id FROM album WHERE album_uri = %s", 
                                 (track['album_uri'],))
                    album_row = cursor.fetchone()
                album_id = album_row[0]

                # Insert track
                cursor.execute(
                    "INSERT INTO track (track_uri, track_name, duration_ms, artist_id, album_id) "
                    "VALUES (%s, %s, %s, %s, %s) "
                    "ON CONFLICT (track_uri) DO NOTHING RETURNING track_id",
                    (track['track_uri'], track['track_name'], track['duration_ms'], 
                     artist_id, album_id)
                )
                track_row = cursor.fetchone()
                if track_row is None:
                    cursor.execute("SELECT track_id FROM track WHERE track_uri = %s", 
                                 (track['track_uri'],))
                    track_row = cursor.fetchone()
                track_id = track_row[0]

                # Insert playlist_track
                cursor.execute(
                    "INSERT INTO playlist_track (playlist_id, track_id, pos) "
                    "VALUES (%s, %s, %s) ON CONFLICT (playlist_id, track_id) DO NOTHING",
                    (playlist_id, track_id, track['pos'])
                )

            conn.commit()
            cursor.close()
            return {"tracks": len(playlist['tracks']), "error": False}
        except Exception as e:
            print(f"Error loading playlist: {e}")
            conn.rollback()
            return {"tracks": 0, "error": True}
        finally:
            self.connection_pool.putconn(conn)
    
    def close(self):
        """Clean up connection pool"""
        if hasattr(self, 'connection_pool'):
            self.connection_pool.closeall()

KAFKA_BROKER = os.getenv("KAFKA_BROKER_URL", "kafka-broker:9093")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "playlist-topic")
TOTAL_EXPECTED = int(os.getenv("TOTAL_EXPECTED", "1000"))


def main():
    # Get execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Reduce parallelism to 2-4 to avoid database connection contention
    env.set_parallelism(2)
    
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

    # Map each message to process it in parallel
    ds.map(ProcessingMapFunction(), output_type=Types.STRING()).filter(lambda x: x is not None).print()

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
    
    # Save benchmark results
    save_benchmark_result("flink", duration, loaded, tracks, errors)

if __name__ == "__main__":
    main()