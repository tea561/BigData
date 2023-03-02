from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, BooleanType, StringType, DoubleType
from pyspark.ml.feature import StringIndexerModel, VectorAssembler, RobustScalerModel
from pyspark.ml.tuning import CrossValidatorModel
from pyspark.sql import functions as F
from pyspark.sql.types import *

import os
from influxdb import InfluxDBClient
from datetime import datetime

dbhost = os.getenv('INFLUXDB_HOST', '127.0.0.1')
dbport = int(os.getenv('INFLUXDB_PORT'))
dbuser = os.getenv('INFLUXDB_USERNAME')
dbpassword = os.getenv('INFLUXDB_PASSWORD')
dbname = os.getenv('INFLUXDB_DATABASE')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
MODEL = os.getenv('MODEL_LOCATION')
SCALER_LOCATION = os.getenv('SCALER_LOCATION')
INDEXER_LOCATION=os.getenv('INDEXER_LOCATION')
columns = ['origin_call', 'call_type_num', 'origin_stand', 'taxi_id', 'start_time', 'start_lat', 'start_lon', 'end_lat', 'end_lon']
nullable_columns = ['origin_call', 'origin_stand', 'taxi_id', 'start_time', 'start_lat', 'start_lon', 'end_lat', 'end_lon']


model = CrossValidatorModel.load(MODEL)
scaler = RobustScalerModel.load(SCALER_LOCATION)
indexer = StringIndexerModel.load(INDEXER_LOCATION)

def process_batch(df1):
    print('wtf')
    for col_name in nullable_columns:
        df1 = df1.withColumn(col_name, F.when(F.isnull(df1[col_name]), 0).otherwise(df1[col_name]).alias(col_name))
    df1 = df1.drop('missing_data')
    df1 = df1.drop('trip_id')
    df1 = df1.drop('day_type')

    prediction.show()
    return prediction

# def write_to_db(data, accuracy):
#     timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
#     measurementData = [
#         {
#             "measurement": KAFKA_TOPIC,
#             "time": timestamp,
#             "fields": {
#                 "start lat": data[0],
#                 "start lon": data[1],
#                 "end lat": data[2],
#                 "end lon": data[3],
#                 "trip duration": data[4],
#                 "predictions": count,
#                 "accuracy": accuracy
#             }
#         }
#     ]
#     print(measurementData)
#     influxClient.write_points(measurementData, time_precision='ms')


if __name__ == '__main__':
    HDFS_DATA = os.getenv('HDFS_URL')

    KAFKA_URL = os.getenv('KAFKA_URL')
    window_duration = os.getenv('WINDOW_DURATION_IN_SECONDS')

    influxClient = InfluxDBClient(dbhost, dbport, dbuser, dbpassword, dbname)
    
    
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
    
    #df1.writeStream.foreach(process_batch).start()
   
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
    prediction.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .start() \
        .awaitTermination() \
        
    spark.stop()
    
    # scaler_model = scaler.fit(split[0])
    # train = scaler_model.transform(split[0])
    # test = scaler_model.transform(split[1])

    
