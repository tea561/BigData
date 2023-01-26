from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys
import os

if __name__ == '__main__':
    # if len(sys.argv) != 4:
    #     print("Usage: app.py <latitude> <longitude> <start_time>")

    first_latitude = float(sys.argv[1])
    first_longitude = float(sys.argv[2])
    second_latitude = float(sys.argv[3])
    second_longitude = float(sys.argv[4])
    start_time = int(sys.argv[5])
    end_time = int(sys.argv[6])

    appName = "Taxi Porto"
    #input = "hdfs://namenode:9000/dir/train.csv"
    input = "hdfs://localhost:9000/dir/train.csv"

    # spark = SparkSession.builder.appName(appName).master("spark://master-spark:7077").getOrCreate()
    spark = SparkSession.builder.appName(appName).master("spark://localhost:7077").getOrCreate()

    spark = SparkSession.builder.appName('data_transformation').master('spark://localhost:7077').getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    dataset = spark.read.option("inferSchema", True).option("header", True).csv(input)

    dataset.printSchema()
    dataset.show(10)

    rows = dataset.count()
    print(f"DataFrame Rows count : {rows}")
    
    # data transformation
    for col in dataset.columns:
        dataset = dataset.withColumnRenamed(col, col.lower())
    
    dataset = dataset.filter(dataset["missing_data"] == False)
    dataset = dataset.withColumnRenamed("timestamp", "start_time")

    print(dataset.where(dataset["start_time"].isNull()).count())

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

    #dataset.repartition(1).write.mode("overwrite").options(header='True', delimiter=',').csv("data/")
    #dataset.repartition(1).write.mode("overwrite").csv("hdfs://localhost:9000/dir/")

    # filter data

    dataset_filtered = dataset.filter(((dataset["start_lat"] > first_latitude) & (dataset["start_lon"] > first_longitude) 
                                        & (dataset["start_lat"] < second_latitude) & (dataset["start_lon"] < second_longitude)
                                        & (dataset["start_time"] > start_time) & (dataset["start_time"] < end_time))
                             | ((dataset["end_lat"] > first_latitude) & (dataset["end_lon"] > first_longitude) 
                                        & (dataset["end_lat"] < second_latitude) & (dataset["end_lon"] < second_longitude)
                                        & (dataset["end_time"] > start_time) & (dataset["end_time"] < end_time)))

    print("Filtered data (20 rows)")
    dataset_filtered.show(20)

    print("Avg, max, min, stdev trip duration grouped by call type")
    dataset_trip_duration_by_call_type = dataset_filtered.groupby("call_type").agg(F.avg("trip_duration"), F.max("trip_duration"), F.min("trip_duration"), F.stddev("trip_duration"))
    dataset_trip_duration_by_call_type.show()


    print("Avg, max, min, stdev trip duration grouped by day type")
    dataset_trip_duration_by_day_type = dataset_filtered.groupby("day_type").agg(F.avg("trip_duration"), F.max("trip_duration"), F.min("trip_duration"), F.stddev("trip_duration"))
    dataset_trip_duration_by_day_type.show()

    print("Avg, max, min, stdev trip duration grouped by taxi_id")
    dataset_trip_duration_by_taxi = dataset_filtered.groupby("taxi_id").agg(F.avg("trip_duration"), F.max("trip_duration"), F.min("trip_duration"), F.stddev("trip_duration"))
    dataset_trip_duration_by_taxi.show()

    print("Taxi driver who spent the most time driving")
    dataset_taxi = dataset_filtered.groupBy("taxi_id").agg(F.sum("trip_duration"))
    dataset_taxi.show(1)
    
    spark.stop()


