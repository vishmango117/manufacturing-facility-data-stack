# Build context = repo root (so we can COPY the shared `common` package).
FROM python:3.12-slim

WORKDIR /app
COPY requirements.edge.txt .
RUN pip install --no-cache-dir -r requirements.edge.txt

COPY common ./common
COPY simulators/ems ./simulators/ems

ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app MODBUS_HOST=0.0.0.0 MODBUS_PORT=5020
EXPOSE 5020
CMD ["python", "-m", "simulators.ems.server"]
