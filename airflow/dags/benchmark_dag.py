from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import uuid

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

def generate_run_id(**context):
    """Generate unique run ID for this benchmark run"""
    run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
    return run_id

with DAG(
    'spotify_benchmark',
    default_args=default_args,
    description='Benchmark all Spotify loaders',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['benchmark'],
) as dag:

    # Generate unique run ID for this benchmark
    gen_run_id = PythonOperator(
        task_id='generate_run_id',
        python_callable=generate_run_id,
    )

    # Truncate data tables (not benchmark_results!)
    def create_truncate_task(task_id):
        return DockerOperator(
            task_id=task_id,
            image='postgres:13',
            command='psql -h postgres -U postgres -d benchmark -c "TRUNCATE TABLE playlist_track, track, album, artist, playlist RESTART IDENTITY CASCADE;"',
            environment={
                'PGPASSWORD': 'postgres',
            },
            network_mode='spotify-loader-benchmark_default',
            auto_remove=True,
            docker_url='unix://var/run/docker.sock',
        )

    # Define all loaders
    loaders = [
        {
            'task_id': 'loader_raw_sql',
            'image': 'spotify-loader-benchmark-loader-celery:latest',
            'command': 'python -u sequential_loader.py',
            'name': 'raw-sql'
        },
        {
            'task_id': 'loader_sequential',
            'image': 'spotify-loader-benchmark-loader-celery:latest',
            'command': 'python -u sequential_loader.py',
            'name': 'sequential'
        },
        {
            'task_id': 'loader_vectorized',
            'image': 'spotify-loader-benchmark-loader-celery:latest',
            'command': 'python -u vectorized_loader.py',
            'name': 'vectorized'
        },
        {
            'task_id': 'loader_multithreaded',
            'image': 'spotify-loader-benchmark-loader-celery:latest',
            'command': 'python -u multithreaded_loader.py',
            'name': 'multithreaded'
        },
        {
            'task_id': 'loader_celery',
            'image': 'spotify-loader-benchmark-loader-celery:latest',
            'command': 'python -u celery_loader.py',
            'name': 'celery',
            'depends_on': ['celery-worker', 'redis']
        },
        {
            'task_id': 'loader_flink',
            'image': 'spotify-loader-benchmark-loader-flink:latest',
            'command': '/opt/flink/bin/flink run -py /opt/flink/flink_loader.py -m flink-jobmanager:8081',
            'name': 'flink',
            'depends_on': ['flink-jobmanager', 'flink-taskmanager']
        },
    ]

    # Build the pipeline: truncate -> loader1 -> truncate -> loader2 -> ...
    previous_task = gen_run_id

    for i, loader_config in enumerate(loaders):
        # Truncate before each loader
        truncate_task = create_truncate_task(f'truncate_before_{loader_config["task_id"]}')
        
        # Create loader task
        loader_task = DockerOperator(
            task_id=loader_config['task_id'],
            image=loader_config['image'],
            command=loader_config['command'],
            environment={
                'RUN_ID': "{{ task_instance.xcom_pull(task_ids='generate_run_id') }}",
                'POSTGRES_HOST': 'postgres',
                'POSTGRES_PORT': '5432',
                'POSTGRES_DB': 'benchmark',
                'POSTGRES_USER': 'postgres',
                'POSTGRES_PASSWORD': 'postgres',
                'KAFKA_BROKER_URL': 'kafka-broker:9093',
                'KAFKA_TOPIC': 'playlist-topic',
                'TOTAL_EXPECTED': '1000',
                'BATCH_SIZE': '100',
                'REDIS_URL': 'redis://redis:6379/0',
                'MAX_WORKERS': '10',
            },
            network_mode='spotify-loader-benchmark_default',
            auto_remove=True,
            docker_url='unix://var/run/docker.sock',
            mount_tmp_dir=False,
        )

        # Chain: previous -> truncate -> loader
        previous_task >> truncate_task >> loader_task
        previous_task = loader_task
