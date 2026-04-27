import os
import uuid
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from docker.types import Mount

PROJECT_DIR = os.getenv("BENCHMARK_PROJECT_DIR", "/root/spotify-loader-benchmark")
DATA_MOUNT = Mount(target="/app/data", source=f"{PROJECT_DIR}/metrics/data", type="bind", read_only=True)
NETWORK = "spotify-loader-benchmark_default"
DOCKER_URL = "unix://var/run/docker.sock"

COMMON_ENV = {
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "benchmark",
    "POSTGRES_USER": "postgres",
    "POSTGRES_PASSWORD": "postgres",
    "KAFKA_BROKER_URL": "kafka-broker:9093",
    "KAFKA_TOPIC": "playlist-topic",
    "TOTAL_EXPECTED": "1000",
    "BATCH_SIZE": "100",
    "REDIS_URL": "redis://redis:6379/0",
    "MAX_WORKERS": "10",
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


def generate_run_id(**context):
    return f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"


with DAG(
    "spotify_benchmark",
    default_args=default_args,
    description="Benchmark all Spotify loaders",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["benchmark"],
) as dag:

    gen_run_id = PythonOperator(
        task_id="generate_run_id",
        python_callable=generate_run_id,
    )

    # Publish all playlists to Kafka before the benchmark starts.
    # Each loader uses a distinct group_id + auto_offset_reset=earliest so
    # they all independently re-read from offset 0.
    produce_data = DockerOperator(
        task_id="produce_data",
        image="spotify-loader-benchmark-metrics-producer:latest",
        command="python -u producer.py",
        environment={"KAFKA_BROKER_URL": "kafka-broker:9093"},
        network_mode=NETWORK,
        auto_remove="success",
        docker_url=DOCKER_URL,
        mount_tmp_dir=False,
        mounts=[DATA_MOUNT],
    )

    RUN_ID_TMPL = "{{ task_instance.xcom_pull(task_ids='generate_run_id') }}"

    loaders = [
        {
            "task_id": "loader_raw_sql",
            "image": "spotify-loader-benchmark-loader-celery:latest",
            "command": "python -u loader_sql.py",
            "mounts": [],
        },
        {
            "task_id": "loader_sequential",
            "image": "spotify-loader-benchmark-loader-celery:latest",
            "command": "python -u sequential_loader.py",
            "mounts": [DATA_MOUNT],
        },
        {
            "task_id": "loader_vectorized",
            "image": "spotify-loader-benchmark-loader-celery:latest",
            "command": "python -u vectorized_loader.py",
            "mounts": [],
        },
        {
            "task_id": "loader_multithreaded",
            "image": "spotify-loader-benchmark-loader-celery:latest",
            "command": "python -u multithreaded_loader.py",
            "mounts": [],
        },
        {
            "task_id": "loader_celery",
            "image": "spotify-loader-benchmark-loader-celery:latest",
            "command": "python -u celery_loader.py",
            "mounts": [],
        },
        {
            "task_id": "loader_flink",
            "image": "spotify-loader-benchmark-loader-flink:latest",
            "command": "/opt/flink/bin/flink run -py /opt/flink/flink_loader.py -m flink-jobmanager:8081",
            "mounts": [],
        },
    ]

    previous_task = gen_run_id >> produce_data

    for loader_cfg in loaders:
        truncate = DockerOperator(
            task_id=f"truncate_before_{loader_cfg['task_id']}",
            image="postgres:13",
            command=(
                'psql -h postgres -U postgres -d benchmark -c '
                '"TRUNCATE TABLE playlist_track, track, album, artist, playlist RESTART IDENTITY CASCADE;"'
            ),
            environment={"PGPASSWORD": "postgres"},
            network_mode=NETWORK,
            auto_remove="success",
            docker_url=DOCKER_URL,
            mount_tmp_dir=False,
        )

        loader = DockerOperator(
            task_id=loader_cfg["task_id"],
            image=loader_cfg["image"],
            command=loader_cfg["command"],
            environment={**COMMON_ENV, "RUN_ID": RUN_ID_TMPL},
            network_mode=NETWORK,
            auto_remove="success",
            docker_url=DOCKER_URL,
            mount_tmp_dir=False,
            mounts=loader_cfg["mounts"],
        )

        previous_task >> truncate >> loader
        previous_task = loader
