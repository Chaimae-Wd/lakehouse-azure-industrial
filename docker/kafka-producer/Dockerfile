FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka==2.6.1

COPY kafka/producer/sensor_producer.py /app/sensor_producer.py

CMD ["python", "/app/sensor_producer.py"]