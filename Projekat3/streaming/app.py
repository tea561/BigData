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
 
        # measurementData = {
        #         "measurement": KAFKA_TOPIC,
        #         "time": timestamp,
        #         "tag": {
        #             "trip": int(row['trip_id'])
        #         },
        #         "fields": {
        #             "trip_id": int(row['trip_id']),
        #             "taxi_id": int(row['taxi_id']),
        #             "start_lat": float(row['start_lat']) if row['start_lat'] != "" else 41.15178,
        #             "start_lon": float(row['start_lon']) if row['start_lon'] != "" else -8.652186,
        #             "end_lat": float(row['end_lat']) if row['end_lat'] != "" else 41.149827,
        #             "end_lon": float(row['end_lon']) if row['end_lon'] != "" else -8.619606,
        #             "trip_duration": float(row['trip_duration']),
        #             "prediction": float(row['prediction'])
        #         }
        #     }
        
        # return Point.from_dict(measurementData)              
        


# def write_to_db(data_row):
#     timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
#     # measurementData = [
#     #     {
#     #         "measurement": KAFKA_TOPIC,
#     #         "time": timestamp,
#     #         "fields": {
#     #             "trip_id": int(data_row['trip_id']),
#     #             "taxi_id": int(data_row['taxi_id']),
#     #             "start_lat": float(data_row['start_lat']) if data_row['start_lat'] != "" else 41.15178,
#     #             "start_lon": float(data_row['start_lon']) if data_row['start_lon'] != "" else -8.652186,
#     #             "end_lat": float(data_row['end_lat']) if data_row['end_lat'] != "" else 41.149827,
#     #             "end_lon": float(data_row['end_lon']) if data_row['end_lon'] != "" else -8.619606,
#     #             "trip_duration": float(data_row['trip_duration']),
#     #             "prediction": float(data_row['prediction'])
#     #         }
#     #     }
#     # ]

#     json_body = [
#         {
#             "measurement": "brushEvents",
#             # "tags": {
#             #     "user": "Carol",
#             #     "brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2f"
#             # },
#             "time": timestamp,
#             "fields": {
#                 "duration": 127
#             }
#         }
#     ]
#     influxClient.write_points(json_body)
#     # print(measurementData)
#     # influxClient.write_points(measurementData, time_precision='ms')

# def process_row(row):
#     row_list = []
#     row_list.append(row)
#     df = spark.createDataFrame(row_list)
#     if df.count() != 0:
#         for col_name in nullable_columns:
#             df = df.withColumn(col_name, F.when(F.isnull(df[col_name]), 0).otherwise(df[col_name]).alias(col_name))
#         df = df.drop('missing_data')
#         df = df.drop('trip_id')
#         df = df.drop('day_type')

#         indexed = indexer.transform(df1)

#         va = VectorAssembler().setInputCols(columns).setOutputCol('features').setHandleInvalid("skip").transform(indexed)

#         scaled = scaler.transform(va)

#         prediction = model.transform(scaled)
#         prediction.foreach(write_to_db)

#         data_row = prediction.head(1)

#         timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

#         json_body = [
#             {
#                 "measurement": "brushEvents",
#                 # "tags": {
#                 #     "user": "Carol",
#                 #     "brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2f"
#                 # },
#                 "time": timestamp,
#                 "fields": {
#                     "duration": 127
#                 }
#             }
#         ]
#         influxClient.write_points(json_body)

#         # prediction.writeStream \
#         #     .outputMode("update") \
#         #     .format("console") \
#         #     .option("truncate", False) \
#         #     .start() \
#         #     .awaitTermination() \


if __name__ == '__main__':
    # HDFS_DATA = os.getenv('HDFS_URL')


    # window_duration = os.getenv('WINDOW_DURATION_IN_SECONDS')    
    
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
    
    # df1.writeStream \
    #     .outputMode("append") \
    #     .foreach(InfluxDBWriter()) \
    #     .start() \
    #     .awaitTermination()
    # df1.writeStream.foreach(process_batch).start()
    
    # df1.writeStream.foreach(process_row).start().awaitTermination()

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
    # spark.streams.awaitAnyTermination()
    # # prediction.writeStream \
    # #     .outputMode("update") \
    # #     .format("console") \
    # #     .option("truncate", False) \
    # #     .start() \
    # #     .awaitTermination() \
        
    # prediction.show()
    
    # scaler_model = scaler.fit(split[0])
    # train = scaler_model.transform(split[0])
    # test = scaler_model.transform(split[1])

    
