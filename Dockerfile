FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py consolidator.py mapper.py enrichment.py formula_engine.py validator.py db.py storage.py migrate_data.py ./
COPY migrations/ ./migrations/

# Create directories for runtime data
RUN mkdir -p /app/deals /app/demo_data

# Expose port
EXPOSE 5000

# Run with gunicorn (production WSGI server)
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "2", "--threads", "4", "--timeout", "300", "app:app"]
