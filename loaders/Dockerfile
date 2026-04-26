FROM python:3.11

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sequential/sequential_loader.py .
COPY vectorized/vectorized_loader.py .

CMD ["python","-u", "vectorized_loader.py"]    