from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, BooleanType, StringType, DoubleType
import sys
from pyspark.sql.functions import *
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os

keyspace = "spark_keyspace"

def writeToCassandra(writeDF, epochId):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="tripduration", keyspace=keyspace) \
        .save()
    
def writePopularStandsToCassandra(writeDF, epochId):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="popular_stands", keyspace=keyspace) \
        .save()


def create_database(cassandra_session):
    cassandra_session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """ % keyspace)

    cassandra_session.set_keyspace(keyspace)

    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS tripduration (
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            count int,
            max float,
            min float,
            avg float,
            stddev float,
            PRIMARY KEY (start_time, end_time)
        )
        """)
    
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS popular_stands (
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            count int,
            end_lon float,
            end_lat float,
            PRIMARY KEY (start_time, end_time)
        )
    """)


if __name__ == '__main__':

    cassandra_host = os.getenv('CASSANDRA_HOST')
    cassandra_port = int(os.getenv('CASSANDRA_PORT'))
    kafka_url = os.getenv('KAFKA_URL')
    topic = os.getenv('KAFKA_TOPIC')
    window_duration = os.getenv('WINDOW_DURATION')
    N = int(os.getenv('N'))

    cassandra_cluster = Cluster([cassandra_host], port=cassandra_port)
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
    appName = "Taxi Porto 2"
    
    conf = SparkConf()
    conf.set("spark.cassandra.connection.host", "cassandra-node")
    conf.set("spark.cassandra.connection.port", cassandra_port)
    spark = SparkSession.builder.config(conf=conf).appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_url) \
        .option("subscribe", topic) \
        .load()
    
    df.printSchema()
    
    df1 = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")
    df1 = df1.withColumn("start_time", to_timestamp(df1['start_time']))
    df1.printSchema()

    grouped_data = df1.groupBy(window("start_time", window_duration))

    statistics = grouped_data.agg(count("*").alias("count"), 
                           avg("trip_duration").alias("avg"), 
                           max("trip_duration").alias("max"), 
                           min("trip_duration").alias("min"), 
                           stddev("trip_duration").alias("stddev"),
                           col("window.start").alias("start_time"),
                           col("window.end").alias("end_time")
                           ).drop("window")
    
    grouped_data_by_end_stand = df1.groupby('end_lon', 'end_lat', window('start_time', window_duration))
    
    popularity =  grouped_data_by_end_stand.agg(count("*").alias("count")) \
        .orderBy(desc("count")) \
        .select(col("count"), col("end_lon"), col("end_lat"), 
                col("window.start").alias("start_time"), col("window.end").alias("end_time"))

    top_taxi_stands = popularity.limit(N)

    # res.writeStream \
    #     .outputMode("update") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .start() \
    #     .awaitTermination()

    query_statistics = statistics.writeStream \
        .outputMode("update") \
        .foreachBatch(writeToCassandra) \
        .start()
    
    query_popularity =top_taxi_stands.writeStream \
        .outputMode("complete") \
        .foreachBatch(writePopularStandsToCassandra) \
        .start()
    

    query_statistics.awaitTermination()
    query_popularity.awaitTermination()
    
    
    spark.stop()

    