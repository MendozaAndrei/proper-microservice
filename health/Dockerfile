FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create directories with proper permissions
RUN mkdir -p /logs /data && chmod -R 777 /logs /data

CMD ["python", "app.py"]