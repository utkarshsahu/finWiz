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

# Use the direct exec form to ensure it's PID 1
# Use the shell form to allow Railway to inject variables if needed
# We use the shell form to ensure the $PORT variable is injected by Railway
# Use the python3 module flag to ensure environment consistency
CMD ["python3", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--proxy-headers", "--forwarded-allow-ips", "*"]