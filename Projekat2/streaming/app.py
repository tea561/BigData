from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, BooleanType, StringType, DoubleType
import sys
from pyspark.sql.functions import *
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def create_database(cassandra_session):
    keyspace = "taxi_porto_keyspace"
    cassandra_session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """ % keyspace)

    cassandra_session.set_keyspace(keyspace)

    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS tripduration (
            time TIMESTAMP,
            max float,
            min float,
            avg float,
            stddev float,
            PRIMARY KEY (time)
        )
        """)


if __name__ == '__main__':

    cassandra_cluster = Cluster("cassandra", port=9042)
    cassandra_session = cassandra_cluster.connect()

    create_database(cassandra_session)

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
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "taxiporto") \
        .load()
    df.printSchema()
    
    df1 = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")
    df1 = df1.withColumn("start_time", to_timestamp(df1['start_time']))
    df1.printSchema()
    grouped_data = df1.groupBy(window("start_time", "1 minute"))

    res = grouped_data.agg(count("*"), avg("trip_duration").alias("avg"), max("trip_duration").alias("max"), min("trip_duration").alias("min"), stddev("trip_duration").alias("stddev"))

    res.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .start() \
        .awaitTermination()
    
    spark.stop()

    