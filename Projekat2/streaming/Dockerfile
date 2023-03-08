FROM bde2020/spark-python-template:3.1.2-hadoop3.2

RUN apk add --no-cache py3-numpy

RUN cd /app pip install -r requirements.txt
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/app.py
ENV SPARK_APPLICATION_ARGS /app/.env
ENV KAFKA_URL kafka:9092
ENV KAFKA_TOPIC taxiporto
ENV CASSANDRA_HOST cassandra-node
ENV CASSANDRA_PORT 9042
ENV WINDOW_DURATION "1 hour"
ENV N 3