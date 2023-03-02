
FROM bde2020/spark-python-template:3.1.2-hadoop3.2

RUN apk add --no-cache py3-numpy
# RUN apk add --no-cache tesseract-ocr python3 py3-numpy && \
#     pip3 install --upgrade pip setuptools wheel && \
#     apk add --no-cache --virtual .build-deps gcc g++ zlib-dev make python3-dev py-numpy-dev jpeg-dev && \
#     pip3 install matplotlib pandas && \
#     apk del .build-deps

ENV SPARK_MASTER spark://spark-master:7077
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/app.py
ENV SPARK_APPLICATION_MAIN_CLASS App3
ENV HDFS_PORTO='hdfs://namenode:9000/data/porto.csv'
ENV MODEL_LOCATION='hdfs://namenode:9000/data/model'
ENV SCALER_LOCATION='hdfs://namenode:9000/data/scaler'
ENV INDEXER_LOCATION='hdfs://namenode:9000/data/indexer'


ADD start.sh /

RUN chmod +x /start.sh
CMD ["/bin/bash", "/start.sh"]