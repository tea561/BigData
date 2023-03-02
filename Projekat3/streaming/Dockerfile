FROM bde2020/spark-python-template:3.1.2-hadoop3.2

RUN apk add --no-cache py3-numpy

ENV SPARK_MASTER spark://spark-master:7077
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/app.py
ENV SPARK_APPLICATION_MAIN_CLASS App3
ENV KAFKA_URL kafka:9092
ENV KAFKA_TOPIC porto
ENV HDFS_URL hdfs://namenode:9000
ENV MODEL_LOCATION='hdfs://namenode:9000/data/model'
ENV SCALER_LOCATION='hdfs://namenode:9000/data/scaler'
ENV INDEXER_LOCATION='hdfs://namenode:9000/data/indexer'


ADD start.sh /

RUN chmod +x /start.sh
CMD ["/bin/bash", "/start.sh"]