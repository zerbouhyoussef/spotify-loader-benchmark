# 🎵 Spotify Loader Benchmark

A comprehensive benchmarking framework for comparing various data loading strategies for ingesting Spotify playlist data from Kafka into PostgreSQL.

## 📊 Overview

This project evaluates the performance of **6 different data loading approaches** processing **100,000 playlists** from Kafka to PostgreSQL:

- **Raw SQL**: Row-by-row inserts with individual queries
- **Sequential**: File-based sequential processing
- **Vectorized**: Batch inserts using pandas DataFrame operations
- **Multithreaded**: Parallel processing with thread pooling
- **Celery**: Distributed task queue with async workers
- **Apache Flink**: Stream processing with PyFlink

## 🏗️ Architecture

```
┌─────────────────┐
│  Kafka Broker   │  8 partitions, 100k playlists
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│              Airflow Orchestration              │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  │
│  │ Truncate  │→ │  Loader   │→ │ Truncate  │  │
│  │   DB      │  │   Task    │  │   DB      │  │
│  └───────────┘  └───────────┘  └───────────┘  │
└─────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────┐
│   PostgreSQL    │  Normalized schema (playlists, tracks, artists, albums)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Grafana      │  Real-time visualization & racing charts
└─────────────────┘
```

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- 8GB+ RAM recommended
- 10GB+ disk space

### Launch the System

```bash
# Start all services
docker compose up -d

# Wait for services to be healthy (~30 seconds)
docker compose ps

# Access the dashboards
# - Airflow UI: http://localhost:8080 (admin/fhTmFTxwFpVh4Cwn)
# - Grafana:    http://localhost:3001 (admin/admin)
# - Flink UI:   http://localhost:8085
```

### Run Benchmark

1. **Open Airflow UI** at http://localhost:8080
2. **Trigger DAG**: Click on `spotify_benchmark` → Trigger DAG
3. **Monitor Progress**: Watch tasks execute sequentially
4. **View Results**: Open Grafana at http://localhost:3001

The benchmark takes **30-60 minutes** to complete all 6 loaders with 100k playlists.

## 📦 Components

### Data Pipeline
- **Kafka**: Message broker with 8 partitions for parallel consumption
- **Producer**: Publishes 100k playlists from `challenge_set.json` to Kafka
- **PostgreSQL**: Target database with normalized schema

### Loaders (Benchmarked)

| Loader | Strategy | Parallelism | Best For |
|--------|----------|-------------|----------|
| **Vectorized** | Pandas batch inserts | Single process | Fastest (30s) |
| **Multithreaded** | Connection pooling | 10 threads | CPU-bound ops |
| **Celery** | Distributed workers | 4 workers | Scalability |
| **Flink** | Stream processing | 2 parallel tasks | Real-time |
| **Raw SQL** | Individual inserts | Single connection | Simplicity |
| **Sequential** | File-based loop | Single process | Low memory |

### Orchestration & Monitoring
- **Airflow**: DAG orchestration with DockerOperator
- **Grafana**: Real-time dashboards with racing visualizations
- **Flink JobManager/TaskManager**: Cluster management for stream processing

## 🎯 Benchmark Metrics

Each loader is measured on:
- **Duration** (seconds)
- **Throughput** (playlists/sec, tracks/sec)
- **Total Records** (playlists, tracks loaded)
- **Error Count**
- **Run ID** (for historical comparison)

Results are stored in `benchmark_results` table and visualized in Grafana with:
- 🏁 Racing line chart showing progress over time
- ⚡ Throughput bar charts
- 📊 Duration comparisons
- 🏆 Winner rankings
- 📋 Complete history table

## ⚙️ Configuration

### Adjust Data Volume

Edit `airflow/dags/benchmark_dag.py`:
```python
COMMON_ENV = {
    "TOTAL_EXPECTED": "100000",  # Number of playlists to process
    "BATCH_SIZE": "100",         # Vectorized batch size
    "MAX_WORKERS": "10",         # Multithreaded worker count
}
```

### Flink Parallelism

Edit `loaders/flink/flink_loader.py`:
```python
env.set_parallelism(2)  # Adjust based on workload
```

Edit `docker-compose.yaml`:
```yaml
environment:
  TASK_MANAGER_NUMBER_OF_TASK_SLOTS: 8  # Task slots available
```

### Database Schema

Schema is defined in `db/init.sql` and includes:
- `playlist` (pid, name, num_tracks, etc.)
- `artist` (artist_uri, artist_name)
- `album` (album_uri, album_name)
- `track` (track_uri, track_name, duration_ms)
- `playlist_track` (many-to-many relationship)
- `benchmark_results` (performance metrics)

## 📈 Performance Optimization

### What Makes Loaders Fast?

1. **Vectorized** (fastest)
   - Batch inserts with `execute_values()`
   - Pre-built DataFrames for deduplication
   - Minimal database round trips

2. **Multithreaded**
   - Connection pooling (`ThreadedConnectionPool`)
   - Parallel processing across threads
   - Efficient resource reuse

3. **Celery**
   - Distributed worker architecture
   - Task queuing with Redis
   - Horizontal scalability

4. **Flink** (after optimization)
   - Connection pooling per parallel task
   - Stream processing framework
   - Ideal for continuous data streams

### Why Some Are Slower?

- **Raw SQL**: Creates new DB connection per playlist (10k+ connections!)
- **Sequential**: No parallelism, file I/O overhead
- **Flink** (before fix): No connection pooling, high parallelism caused contention

## 🔧 Troubleshooting

### Kafka Consumer Issues
If loaders skip messages, check consumer groups:
```bash
# List consumer groups
docker exec kafka-broker kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Reset offsets
docker exec kafka-broker kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group <group-name> --reset-offsets --to-earliest --topic playlist-topic --execute
```

### Airflow DAG Not Appearing
```bash
# Reserialize DAGs
docker exec airflow-scheduler-server airflow dags reserialize

# Check logs
docker logs airflow-scheduler-server
```

### Grafana Dashboard Not Updating
- Verify Postgres data source connection
- Check `benchmark_results` table has data
- Adjust dashboard refresh rate (top-right corner)

## 📝 Project Structure

```
spotify-loader-benchmark/
├── airflow/
│   ├── dags/
│   │   └── benchmark_dag.py       # Main orchestration DAG
│   ├── Dockerfile
│   └── requirements.txt
├── loaders/
│   ├── raw_sql/
│   │   └── loader_sql.py
│   ├── sequential/
│   │   └── sequential_loader.py
│   ├── vectorized/
│   │   └── vectorized_loader.py
│   ├── multithreaded/
│   │   └── multithreaded_loader.py
│   ├── Celery/
│   │   ├── celery_loader.py
│   │   └── tasks.py
│   ├── flink/
│   │   ├── flink_loader.py
│   │   ├── flink-conf.yaml
│   │   └── Dockerfile
│   ├── benchmark_utils.py         # Shared result saving
│   └── Dockerfile
├── metrics/
│   ├── producer.py                # Kafka producer
│   └── data/
│       └── challenge_set.json     # 1M playlists dataset
├── grafana/
│   ├── provisioning/
│   └── dashboards/
│       └── benchmark.json         # Racing dashboard
├── db/
│   └── init.sql                   # PostgreSQL schema
└── docker-compose.yaml            # Service orchestration
```

## 🎓 Key Learnings

### Connection Management
- **Always use connection pooling** for parallel workloads
- Creating connections is expensive (100ms+ per connection)
- Reusing connections is 10-100x faster

### Batch Operations
- **Batch inserts drastically reduce round trips**
- `execute_values()` is 10x faster than individual `execute()`
- Pre-computing deduplication saves duplicate key violations

### Parallelism Trade-offs
- **More threads ≠ faster** (diminishing returns, contention)
- Database locks become bottleneck with high concurrency
- Optimal parallelism: 2-10 workers depending on workload

### Stream Processing
- **Flink excels at continuous streams**, not batch jobs
- Connection pooling is critical for per-record processing
- Trade-off: Framework overhead vs. processing capabilities

## 📜 License

MIT License - See LICENSE file for details

## 🤝 Contributing

Contributions welcome! Feel free to:
- Add new loader implementations
- Optimize existing loaders
- Enhance visualizations
- Improve documentation

---

**Built with:** Kafka • PostgreSQL • Apache Flink • Airflow • Grafana • Python • Docker
