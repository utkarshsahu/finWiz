# Use a slim Python 3.13 image
FROM python:3.13-slim

# Prevent Python from buffering logs and writing .pyc files
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
# Ensure the /app directory is in the python path
ENV PYTHONPATH=/app

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Keeping your specific pdfplumber requirement
RUN pip install --no-deps pdfplumber

# Copy the rest of the application code
COPY . .

# Railway ignores EXPOSE, but it's good for documentation
EXPOSE 8080

# CRITICAL: Use shell form to allow environment variable expansion ($PORT)
# If Railway doesn't provide a PORT, it defaults to 8080 for safety
CMD sh -c "python -m uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}"