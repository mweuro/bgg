FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

RUN useradd --create-home --shell /bin/bash app
USER app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src
ENV PGDATABASE=bgg

CMD ["python", "src/main.py"]