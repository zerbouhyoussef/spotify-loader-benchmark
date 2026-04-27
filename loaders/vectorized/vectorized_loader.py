import os
import json
import psycopg2
from psycopg2.extras import execute_values
import time
from dotenv import load_dotenv
from kafka import KafkaConsumer
from benchmark_utils import save_benchmark_result

load_dotenv()

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
TOTAL_EXPECTED = int(os.getenv("TOTAL_EXPECTED", "100"))

start = time.time()
loaded = 0
errors = 0
rows = 0

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
    group_id="vectorized-loader",
    consumer_timeout_ms=30000,
    max_poll_records=100,
    fetch_min_bytes=1,
)

playlist_buffer = []
track_buffer = []
artist_buffer = []
album_buffer = []
playlist_track_buffer = []

def extract_playlist_row(playlist):
    return (playlist['pid'], playlist['name'], playlist['num_tracks'], playlist['num_holdouts'], playlist['num_samples'])

def extract_artist_row(track):
    return (track['artist_uri'], track['artist_name'])

def extract_album_row(track):
    return (track['album_uri'], track['album_name'])

def extract_track_row(track, artist_id, album_id):

    return (track['track_uri'], track['track_name'], track['duration_ms'], artist_id, album_id)

def fetch_artist_album_ids(artist_buffer_unique, album_buffer_unique):
    artist_id_map = {}
    album_id_map = {}
    if artist_buffer_unique:
        cursor.execute(
            "SELECT artist_uri, artist_id FROM artist WHERE artist_uri = ANY(%s)",
            ([a[0] for a in artist_buffer_unique],)
        )
        artist_id_map = dict(cursor.fetchall())
    if album_buffer_unique:
        cursor.execute(
            "SELECT album_uri, album_id FROM album WHERE album_uri = ANY(%s)",
            ([a[0] for a in album_buffer_unique],)
        )
        album_id_map = dict(cursor.fetchall())
    return artist_id_map, album_id_map

for message in consumer:
    try:
        playlist = json.loads(message.value)
        
        # Skip playlists with no tracks early
        if not playlist.get('tracks', []):
            continue
        
        # Validate required playlist fields
        required_fields = ['pid', 'name', 'num_tracks', 'num_holdouts', 'num_samples']
        if not all(field in playlist for field in required_fields):
            continue
            
        playlist_buffer.append(extract_playlist_row(playlist))

        for track in playlist.get('tracks', []):
            artist_buffer.append(extract_artist_row(track))
            album_buffer.append(extract_album_row(track))
            playlist_track_buffer.append((playlist['pid'], track))

        # Only insert when we reach the batch size (in playlists)
        if len(playlist_buffer) >= BATCH_SIZE:
            # Insert artists (deduplicated)
            artist_buffer_unique = list({(a[0], a[1]) for a in artist_buffer})
            if artist_buffer_unique:
                execute_values(
                    cursor,
                    """
                    INSERT INTO artist (artist_uri, artist_name)
                    VALUES %s
                    ON CONFLICT (artist_uri) DO NOTHING
                    """,
                    artist_buffer_unique,
                )
            # Insert albums (deduplicated)
            album_buffer_unique = list({(a[0], a[1]) for a in album_buffer})
            if album_buffer_unique:
                execute_values(
                    cursor,
                    """
                    INSERT INTO album (album_uri, album_name)
                    VALUES %s
                    ON CONFLICT (album_uri) DO NOTHING
                    """,
                    album_buffer_unique,
                )

            # Insert playlists (deduplicated)
            execute_values(
                cursor,
                """
                INSERT INTO playlist (pid, name, num_tracks, num_holdouts, num_samples)
                VALUES %s
                ON CONFLICT (pid) DO NOTHING
                """,
                playlist_buffer,
            )

            # COMMIT before select to avoid transaction/cursor state issues
            conn.commit()  # flush inserts before fetching IDs

            # Fetch artist_id and album_id mappings after insertion
            artist_id_map, album_id_map = fetch_artist_album_ids(artist_buffer_unique, album_buffer_unique)


            track_buffer = []
            track_seen = set()
            track_record_map = {}
            for pid, track in playlist_track_buffer:
                track_record_map[track['track_uri']] = track
            for pid, track in playlist_track_buffer:
                track_uri = track['track_uri']
                if track_uri not in track_seen:
                    artist_id = artist_id_map.get(track['artist_uri'])
                    album_id = album_id_map.get(track['album_uri'])
                    if artist_id is not None and album_id is not None:
                        track_buffer.append((track_uri, track['track_name'], track['duration_ms'], artist_id, album_id))
                        track_seen.add(track_uri)

            if track_buffer:
                execute_values(
                    cursor,
                    """
                    INSERT INTO track (track_uri, track_name, duration_ms, artist_id, album_id)
                    VALUES %s
                    ON CONFLICT (track_uri) DO NOTHING
                    """,
                    track_buffer,
                )

            # Map playlist pid to playlist_id
            cursor.execute("SELECT pid, playlist_id FROM playlist WHERE pid = ANY(%s)", ([p[0] for p in playlist_buffer],))
            playlist_id_map = dict(cursor.fetchall())

            # Map track_uri to track_id
            cursor.execute("SELECT track_uri, track_id FROM track WHERE track_uri = ANY(%s)", ([t[0] for t in track_buffer],))
            track_id_map = dict(cursor.fetchall())

            # Build playlist_track rows with playlist_id and track_id, plus pos
            playlist_track_rows = []
            for pid, track in playlist_track_buffer:
                track_uri = track['track_uri']
                pos = track['pos']
                playlist_id = playlist_id_map.get(pid)
                track_id = track_id_map.get(track_uri)
                if playlist_id is not None and track_id is not None:
                    playlist_track_rows.append((playlist_id, track_id, pos))

            # Insert playlist_track (no dedup; trust ON CONFLICT to prevent dupes if needed)
            if playlist_track_rows:
                execute_values(
                    cursor,
                    """
                    INSERT INTO playlist_track (playlist_id, track_id, pos)
                    VALUES %s
                    ON CONFLICT (playlist_id, track_id) DO NOTHING
                    """,
                    playlist_track_rows,
                )

            conn.commit()

            loaded += len(playlist_buffer)
            rows += len(track_buffer)
            playlist_buffer = []
            track_buffer = []
            artist_buffer = []
            album_buffer = []
            playlist_track_buffer = []

        if loaded >= int(TOTAL_EXPECTED):
            print(f"Reached expected number of playlists ({TOTAL_EXPECTED}). Stopping consumer loop.")
            break

    except Exception as e:
        print(f"Error loading batch: {e}")
        conn.rollback()
        errors += 1
        continue

# Final flush: process any remaining buffered data
if playlist_buffer:
    try:
        # Insert artists (deduplicated)
        artist_buffer_unique = list({(a[0], a[1]) for a in artist_buffer})
        if artist_buffer_unique:
            execute_values(
                cursor,
                """
                INSERT INTO artist (artist_uri, artist_name)
                VALUES %s
                ON CONFLICT (artist_uri) DO NOTHING
                """,
                artist_buffer_unique,
            )
        # Insert albums (deduplicated)
        album_buffer_unique = list({(a[0], a[1]) for a in album_buffer})
        if album_buffer_unique:
            execute_values(
                cursor,
                """
                INSERT INTO album (album_uri, album_name)
                VALUES %s
                ON CONFLICT (album_uri) DO NOTHING
                """,
                album_buffer_unique,
            )

        # Insert playlists
        execute_values(
            cursor,
            """
            INSERT INTO playlist (pid, name, num_tracks, num_holdouts, num_samples)
            VALUES %s
            ON CONFLICT (pid) DO NOTHING
            """,
            playlist_buffer,
        )

        conn.commit()

        # Fetch artist_id and album_id mappings
        artist_id_map, album_id_map = fetch_artist_album_ids(artist_buffer_unique, album_buffer_unique)

        # Build tracks buffer
        track_buffer = []
        track_seen = set()
        for pid, track in playlist_track_buffer:
            track_uri = track['track_uri']
            if track_uri not in track_seen:
                artist_id = artist_id_map.get(track['artist_uri'])
                album_id = album_id_map.get(track['album_uri'])
                if artist_id is not None and album_id is not None:
                    track_buffer.append((track_uri, track['track_name'], track['duration_ms'], artist_id, album_id))
                    track_seen.add(track_uri)

        if track_buffer:
            execute_values(
                cursor,
                """
                INSERT INTO track (track_uri, track_name, duration_ms, artist_id, album_id)
                VALUES %s
                ON CONFLICT (track_uri) DO NOTHING
                """,
                track_buffer,
            )

        # Map playlist pid to playlist_id
        cursor.execute("SELECT pid, playlist_id FROM playlist WHERE pid = ANY(%s)", ([p[0] for p in playlist_buffer],))
        playlist_id_map = dict(cursor.fetchall())

        # Map track_uri to track_id
        cursor.execute("SELECT track_uri, track_id FROM track WHERE track_uri = ANY(%s)", ([t[0] for t in track_buffer],))
        track_id_map = dict(cursor.fetchall())

        # Build playlist_track rows
        playlist_track_rows = []
        for pid, track in playlist_track_buffer:
            track_uri = track['track_uri']
            pos = track['pos']
            playlist_id = playlist_id_map.get(pid)
            track_id = track_id_map.get(track_uri)
            if playlist_id is not None and track_id is not None:
                playlist_track_rows.append((playlist_id, track_id, pos))

        if playlist_track_rows:
            execute_values(
                cursor,
                """
                INSERT INTO playlist_track (playlist_id, track_id, pos)
                VALUES %s
                ON CONFLICT (playlist_id, track_id) DO NOTHING
                """,
                playlist_track_rows,
            )

        conn.commit()

        loaded += len(playlist_buffer)
        rows += len(track_buffer)
    except Exception as e:
        print(f"Error in final flush: {e}")
        conn.rollback()
        errors += 1

duration = time.time() - start

print("Done loading data!", flush=True)
print(f"Duration: {duration:.2f}s | Playlists: {loaded} | Tracks: {rows} | Errors: {errors}")

save_benchmark_result("vectorized", duration, loaded, rows, errors)

conn.close()
consumer.close()