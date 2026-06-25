FROM python:3.12-slim

WORKDIR /app
COPY requirements.edge.txt .
RUN pip install --no-cache-dir -r requirements.edge.txt psycopg2-binary

COPY flink/jobs /app/jobs

ENV PYTHONUNBUFFERED=1
CMD ["python3", "/app/jobs/telemetry_consumer.py"]
