from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, BooleanType, StringType, DoubleType
from pyspark.ml.feature import StringIndexerModel, VectorAssembler, RobustScalerModel
from pyspark.ml.tuning import CrossValidatorModel
from pyspark.sql import functions as F
from pyspark.sql.types import *

import os
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

# dbhost = os.getenv('INFLUXDB_HOST', '127.0.0.1')
# dbport = int(os.getenv('INFLUXDB_PORT'))
# dbuser = os.getenv('INFLUXDB_USERNAME')
# dbpassword = os.getenv('INFLUXDB_PASSWORD')
# dbname = os.getenv('INFLUXDB_DATABASE')
# KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
# MODEL = os.getenv('MODEL_LOCATION')
# SCALER_LOCATION = os.getenv('SCALER_LOCATION')
# INDEXER_LOCATION=os.getenv('INDEXER_LOCATION')


dbhost = 'influxdb'
dbport = 8086
dbuser = 'admin'
dbpassword = 'admin'
dbname = 'taxiportodb'
columns = ['origin_call', 'call_type_num', 'origin_stand', 'taxi_id', 'start_time', 'start_lat', 'start_lon', 'end_lat', 'end_lon']
nullable_columns = ['origin_call', 'origin_stand', 'taxi_id', 'start_time', 'start_lat', 'start_lon', 'end_lat', 'end_lon']
MODEL_LOCATION='hdfs://namenode:9000/data/model'
SCALER_LOCATION='hdfs://namenode:9000/data/scaler'
INDEXER_LOCATION='hdfs://namenode:9000/data/indexer'
KAFKA_URL = 'kafka:9092'
KAFKA_TOPIC = 'porto'

model = CrossValidatorModel.load(MODEL_LOCATION)
scaler = RobustScalerModel.load(SCALER_LOCATION)
indexer = StringIndexerModel.load(INDEXER_LOCATION)

#influxClient = InfluxDBClient(dbhost, dbport, dbuser, dbpassword, dbname)


class InfluxDBWriter:
    def __init__(self):
        self._org = 'taxiportodb'
        self._token = '2c83186a-caab-425a-9594-9d4c00544939'
        self.client = InfluxDBClient(
            url = "http://influxdb:8086", token=self._token, org = self._org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def open(self, partition_id, epoch_id):
        print("Opened %d, %d" % (partition_id, epoch_id))
        return True

    def process(self, row):
        self.write_api.write(bucket='taxiportodb',
                             record=self._row_to_line_protocol(row))

    def close(self, error):
        self.write_api.__del__()
        self.client.__del__()
        print("Closed with error: %s" % str(error))

    def _row_to_line_protocol(self, row):
        print(row)
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        return Point.measurement(KAFKA_TOPIC).tag("measure", KAFKA_TOPIC) \
                    .field("start_lat", float(row['start_lat'])) \
                    .field("end_lat", float(row['start_lat'])) \
                    .field("start_lon", float(row['start_lon'])) \
                    .field("end_lon", float(row['end_lon'])) \
                    .field("trip_duration", float(row['trip_duration'])) \
                    .field("prediction", float(row['prediction'])) \
                    .time(timestamp, write_precision='ms')

if __name__ == '__main__':    
    schema = StructType([
                StructField("trip_id",LongType(),True),
                StructField("call_type", StringType(),True),
                StructField("origin_call", IntegerType(),True),
                StructField("origin_stand", IntegerType(),True),
                StructField("taxi_id", IntegerType(),True),
                StructField("start_time", IntegerType(),True),
                StructField("end_time", IntegerType(),True),
                StructField("day_type",StringType(),True),
                StructField("missing_data",BooleanType(),True),
                StructField("start_lon", DoubleType(),True),
                StructField("start_lat",DoubleType(),True),
                StructField("end_lon", DoubleType(),True),
                StructField("end_lat",DoubleType(),True),
                StructField("trip_duration", DoubleType(),True),
            ])  
    appName = "Taxi Porto"
    spark = SparkSession.builder.appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_URL) \
        .option("subscribe", "taxiporto") \
        .load()
    df.printSchema()

    df1 = df.selectExpr("CAST(value AS STRING)").select(F.from_json(F.col("value"), schema).alias("data")).select("data.*")
    

    for col_name in nullable_columns:
        df1 = df1.withColumn(col_name, F.when(F.isnull(df1[col_name]), 0).otherwise(df1[col_name]).alias(col_name))
    df1 = df1.drop('missing_data')
    df1 = df1.drop('trip_id')
    df1 = df1.drop('day_type')
    
    indexed = indexer.transform(df1)

    va = VectorAssembler().setInputCols(columns).setOutputCol('features').setHandleInvalid("skip").transform(indexed)
    
    scaled = scaler.transform(va)

    prediction = model.transform(scaled)

    prediction.printSchema()

    query = prediction.writeStream \
        .foreach(InfluxDBWriter()) \
        .start()
    query.awaitTermination()

    
