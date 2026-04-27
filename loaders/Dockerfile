FROM python:3.11

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY benchmark_utils.py .
COPY raw_sql/loader_sql.py .
COPY sequential/sequential_loader.py .
COPY vectorized/vectorized_loader.py .
COPY multithreaded/multithreaded_loader.py .
COPY Celery/tasks.py .
COPY Celery/celery_loader.py .

CMD ["python","-u", "celery_loader.py"]