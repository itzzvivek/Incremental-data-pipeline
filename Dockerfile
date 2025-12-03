FROM openjdk:11

# Python + pip
RUN apt-get update && apt-get install -y python3 python3-pip

WORKDIR /app

COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . /app/

ENV PYTHONUNBUFFERED=1

CMD ["python3", "/app/scripts/run_pipeline.py", "--once"]