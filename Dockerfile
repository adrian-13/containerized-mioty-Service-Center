
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

# Create necessary directories
RUN mkdir -p certs logs

# Ensure config files exist and set permissions
RUN touch endpoints.json .env && \
    chmod 644 bssci_config.py endpoints.json && \
    chown root:root bssci_config.py endpoints.json && \
    chmod 666 .env

# Expose ports
EXPOSE 16018 5000

# Default command
CMD ["python", "web_main.py"]
