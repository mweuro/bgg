FROM mcr.microsoft.com/playwright/python:latest

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Install browsers
RUN playwright install --with-deps

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash app
USER app

# Ensure unbuffered output for proper logging in Docker
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

CMD ["python", "src/main.py"]