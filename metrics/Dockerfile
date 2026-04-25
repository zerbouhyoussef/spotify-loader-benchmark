FROM python:3.11

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY data/challenge_set.json challenge_set.json

COPY producer.py .

CMD ["python", "producer.py"]