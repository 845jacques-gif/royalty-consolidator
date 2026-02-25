FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py consolidator.py mapper.py enrichment.py formula_engine.py validator.py db.py storage.py migrate_data.py forecast.py delta.py ./
COPY migrations/ ./migrations/

# Create directories for runtime data
RUN mkdir -p /app/deals /app/demo_data

# Expose port (Cloud Run sets PORT env var, default 8080)
EXPOSE 8080

# Run with gunicorn — uses $PORT from Cloud Run, falls back to 8080
CMD gunicorn --bind "0.0.0.0:${PORT:-8080}" --workers 2 --threads 4 --timeout 900 app:app
