FROM python:3.11

WORKDIR /app

COPY . .

COPY data/challenge_set.json /app/challenge_set.json

RUN pip install -r requirements.txt

CMD ["python", "producer.py"]