FROM python:3.12-slim

WORKDIR /app
COPY erp/api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY erp/api/main.py .

ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
