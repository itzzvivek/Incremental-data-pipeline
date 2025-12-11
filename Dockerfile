FROM python:3.11-slim

# Install system tools
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy app requirements
COPY requirements.txt /app/

# Install python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy everything
COPY . /app/

ENV PYTHONUNBUFFERED=1

CMD ["python3", "/app/scripts/run_pipeline.py", "--once"]
