
FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openssl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Create necessary directories and ensure entrypoint is executable
RUN mkdir -p certs logs data && \
    chmod +x /app/docker-entrypoint.sh

# Expose ports
EXPOSE 16018 5000

ENTRYPOINT ["./docker-entrypoint.sh"]
CMD ["python", "web_main.py"]
