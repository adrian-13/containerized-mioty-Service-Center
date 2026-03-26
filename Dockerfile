
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

# Create necessary directories and ensure config files exist
RUN mkdir -p certs logs && \
    touch endpoints.json .env && \
    cp -n users.default.json users.json 2>/dev/null || true && \
    chmod 644 bssci_config.py endpoints.json && \
    chmod 666 .env

# Expose ports
EXPOSE 16018 5000

# Copy defaults for mounted volumes that may be empty, then start
CMD ["sh", "-c", "[ -s users.json ] || cp users.default.json users.json; [ -s endpoints.json ] || echo '{}' > endpoints.json; exec python web_main.py"]
