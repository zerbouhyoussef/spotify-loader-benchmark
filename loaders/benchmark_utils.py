"""
Shared utilities for benchmark loaders
"""
import os
import psycopg2
from datetime import datetime


def save_benchmark_result(loader_name, duration_s, playlists, tracks, errors):
    """
    Save benchmark results to the database.
    
    Args:
        loader_name: Name of the loader (e.g., 'raw-sql', 'flink', 'celery')
        duration_s: Total duration in seconds
        playlists: Number of playlists loaded
        tracks: Number of tracks loaded
        errors: Number of errors encountered
    """
    run_id = os.getenv("RUN_ID", f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    
    db_config = {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": os.getenv("POSTGRES_DB", "benchmark"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    }
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO benchmark_results 
            (loader_name, duration_s, playlists, tracks, errors, run_id) 
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (loader_name, duration_s, playlists, tracks, errors, run_id)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✓ Saved benchmark result: {loader_name} - Run ID: {run_id}")
    except Exception as e:
        print(f"✗ Failed to save benchmark result: {e}")
