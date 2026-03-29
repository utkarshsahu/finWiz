# Use a slim Python 3.10+ image
FROM python:3.11-slim

# Prevent Python from buffering logs (forces logs to appear in Railway instantly)
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set working directory
WORKDIR /app

# Install system dependencies (required for some PDF or data libraries)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-deps pdfplumber

# Copy the rest of the application code
COPY . .

# Expose the port (Railway uses this internally)
EXPOSE 8000

# Start the application
# Note: Using the ${PORT:-8000} syntax to handle Railway's dynamic port assignment
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"]
