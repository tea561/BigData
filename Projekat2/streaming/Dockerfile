FROM bde2020/spark-python-template:latest

RUN cd /app pip install -r requirements.txt
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/app.py
ENV SPARK_APPLICATION_ARGS /app/.env