FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source files
COPY proto/ ./proto/
COPY raftkv/ ./raftkv/
COPY generate_proto.py main.py client.py ./

# Build-stage protobuf generation
RUN python generate_proto.py

# Environment setup
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

EXPOSE 50051

ENTRYPOINT ["python", "main.py"]
