FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir psycopg2-binary==2.9.10

COPY scripts/generate_demo_data.py /app/generate_demo_data.py

CMD ["python", "/app/generate_demo_data.py"]