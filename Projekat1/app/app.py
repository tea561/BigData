from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import *
import sys
import os
from dotenv import load_dotenv

if __name__ == '__main__':

    load_dotenv()

    first_latitude = float(os.getenv('FIRST_LATITUDE'))
    first_longitude = float(os.getenv('FIRST_LONGITUDE'))
    second_latitude = float(os.getenv('SECOND_LATITUDE'))
    second_longitude = float(os.getenv('SECOND_LONGITUDE'))
    start_time = int(os.getenv('START_TIME'))
    end_time = int(os.getenv('END_TIME'))

    appName = "Taxi Porto"

    input = "hdfs://namenode:9000/dir/train.csv"
    spark = SparkSession.builder.appName(appName).master("spark://spark-master:7077").getOrCreate()

    
    #input = "hdfs://localhost:9000/dir/train.csv"
    #spark = SparkSession.builder.appName(appName).master("local[2]").getOrCreate()

    # spark = SparkSession.builder.appName(appName).master("spark://localhost:7077").getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    dataset = spark.read.option("inferSchema", True).option("header", True).csv(input)

    dataset.printSchema()
    dataset.show(10)    

    for col in dataset.columns:
        dataset = dataset.withColumnRenamed(col, col.lower())
    
    #drop missing values
    dataset = dataset.filter(dataset["missing_data"] == False)
    dataset = dataset.withColumnRenamed("timestamp", "start_time")

    # check missing values
    print(dataset.where(dataset["start_time"].isNull()).count())


    # data transformation
    dataset = dataset.withColumn("coordinates", F.regexp_replace("polyline", "[\[|\]]", ""))

    dataset = dataset.withColumn("start_lon", F.split(dataset["coordinates"], ",").getItem(0).cast('double'))
    dataset = dataset.withColumn("start_lat", F.split(dataset["coordinates"], ",").getItem(1).cast('double'))
    dataset = dataset.withColumn("end_lon", F.reverse(F.split(dataset["coordinates"], ",")).getItem(1).cast('double'))
    dataset = dataset.withColumn("end_lat", F.reverse(F.split(dataset["coordinates"], ",")).getItem(0).cast('double'))

    dataset = dataset.withColumn("array_of_coordinates", F.split(dataset["coordinates"], ","))
    dataset = dataset.withColumn("trip_duration", F.size(F.col("array_of_coordinates")) * 7.5 )

    dataset = dataset.withColumn("end_time", dataset["start_time"] + dataset["trip_duration"])
    dataset = dataset.drop("polyline")
    dataset = dataset.drop("coordinates")
    dataset = dataset.drop("array_of_coordinates")

    dataset.show(10)

    # filter data

    dataset_filtered = dataset.filter(((dataset["start_lat"] > first_latitude) & (dataset["start_lon"] > first_longitude) 
                                        & (dataset["start_lat"] < second_latitude) & (dataset["start_lon"] < second_longitude)
                                        & (dataset["start_time"] > start_time) & (dataset["start_time"] < end_time))
                             | ((dataset["end_lat"] > first_latitude) & (dataset["end_lon"] > first_longitude) 
                                        & (dataset["end_lat"] < second_latitude) & (dataset["end_lon"] < second_longitude)
                                        & (dataset["end_time"] > start_time) & (dataset["end_time"] < end_time)))

    print("Filtered data")
    dataset_filtered.show(20)

    print("Average trip duration grouped by call type")
    dataset_trip_duration_by_call_type = dataset_filtered.groupBy("call_type").agg(F.avg("trip_duration"))
    dataset_trip_duration_by_call_type.show()

    
    trip_duration_stddev = dataset_filtered.groupBy("origin_stand").agg(F.stddev("trip_duration").alias("trip_duration_stddev"))
    trip_duration_stddev = trip_duration_stddev.sort("trip_duration_stddev", ascending=False)
    origin_stand = trip_duration_stddev.collect()[0]
    result = "Taxi stand with the widest range of trip duration is " + str(origin_stand.asDict()["origin_stand"]) \
            + " (stddev =  " + str(origin_stand.asDict()['trip_duration_stddev']) + ")\n"
    with open("output.txt", 'w') as f:
        f.write(result)

    dataset_max = dataset_filtered.filter(dataset_filtered["call_type"] == 'B').groupBy('origin_stand').agg(count('trip_id')\
        .alias('num_of_trips'), max('trip_duration').alias('max_trip_time'))
    dataset_max_duration = dataset_max.sort("max_trip_time", ascending=False)
    dataset_max_count = dataset_max.sort("num_of_trips", ascending=False)
    max_duration = dataset_max_duration.collect()[0]
    max_count = dataset_max_count.collect()[0]
    result = "The longest trip was " + str(max_duration.asDict()['max_trip_time'] / 3600) + " hours from taxi stand " \
            + str(max_duration.asDict()['origin_stand']) + "\n"
    result_count = "The highest number of trips (" + str(max_count.asDict()['num_of_trips']) + ') started from taxi stand ' \
            + str(max_count.asDict()['origin_stand']) + "\n"
    with open("output.txt", 'a') as f:
        f.write(result) 
        f.write(result_count)    

    dataset_taxi = dataset_filtered.groupBy("taxi_id").agg(F.sum("trip_duration").alias('trip_duration_sum'))
    dataset_taxi = dataset_taxi.filter(dataset_taxi["trip_duration_sum"] > 0).sort("trip_duration_sum", ascending=True)
    taxi_driver = dataset_taxi.collect()[0]
    result = 'Taxi driver who spent the least time driving is ' + str(taxi_driver.asDict()['taxi_id']) \
            + ' (' + str(taxi_driver.asDict()['trip_duration_sum'] / 60) + ' minutes)\n'
    
    with open("output.txt", 'a') as f:
        f.write(result)


    spark.stop()


