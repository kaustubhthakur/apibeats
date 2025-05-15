FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY worker.py .

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Command to run the application
CMD ["uvicorn", "worker:app", "--host", "0.0.0.0", "--port", "8080"]