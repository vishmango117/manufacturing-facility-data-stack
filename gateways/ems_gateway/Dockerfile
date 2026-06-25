# Build context = repo root (so we can COPY the shared `common` package).
FROM python:3.12-slim

WORKDIR /app
COPY requirements.edge.txt .
RUN pip install --no-cache-dir -r requirements.edge.txt

COPY common ./common
COPY gateways/__init__.py ./gateways/__init__.py
COPY gateways/ems_gateway ./gateways/ems_gateway

ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
CMD ["python", "-m", "gateways.ems_gateway.publish"]
