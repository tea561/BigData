from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import *
import sys
import os

if __name__ == '__main__':

    appName = "Taxi Porto"

    input = "train.csv"
    spark = SparkSession.builder.appName(appName).master("local[2]").getOrCreate()

    # spark = SparkSession.builder.appName(appName).master("spark://localhost:7077").getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    dataset = spark.read.option("inferSchema", True).option("header", True).csv(input)


    for col in dataset.columns:
        dataset = dataset.withColumnRenamed(col, col.lower())
    
    #drop missing values
    dataset = dataset.filter(dataset["missing_data"] == False)
    dataset = dataset.withColumnRenamed("timestamp", "start_time")

    # data transformation
    dataset = dataset.withColumn("col1", expr("substring(polyline, 3, length(polyline) - 2)"))
    dataset = dataset.withColumn("col2", F.regexp_replace("col1", ",\[", ""))
    dataset = dataset.withColumn("array_of_coordinates", F.split(dataset["col2"], "]"))
    dataset = dataset.select("*", posexplode("array_of_coordinates"))
    dataset = dataset.withColumn("lon", F.split(dataset["col"], ",").getItem(0).cast('double'))
    dataset = dataset.withColumn("lat", F.split(dataset["col"], ",").getItem(1).cast('double'))
    dataset = dataset.withColumn("start_time", dataset["start_time"] + 15 * dataset["pos"])

    # dataset = dataset.withColumn("start_lon", F.split(dataset["coordinates"], ",").getItem(0).cast('double'))
    # dataset = dataset.withColumn("start_lat", F.split(dataset["coordinates"], ",").getItem(1).cast('double'))
    # dataset = dataset.withColumn("end_lon", F.reverse(F.split(dataset["coordinates"], ",")).getItem(1).cast('double'))
    # dataset = dataset.withColumn("end_lat", F.reverse(F.split(dataset["coordinates"], ",")).getItem(0).cast('double'))

    # dataset = dataset.withColumn("array_of_coordinates", F.split(dataset["coordinates"], ","))
    # dataset = dataset.withColumn("trip_duration", F.size(F.col("array_of_coordinates")) * 7.5 )

    # dataset = dataset.withColumn("end_time", dataset["start_time"] + dataset["trip_duration"])
    dataset = dataset.drop("polyline")
    dataset = dataset.drop("col1")
    dataset = dataset.drop("array_of_coordinates")
    dataset = dataset.drop("pos")
    dataset = dataset.drop("col")
    dataset = dataset.drop("col2")
    dataset = dataset.na.drop(subset=["lat", "lon"])
    dataset = dataset.sort("start_time")

    dataset.repartition(1).write.mode("overwrite").options(header='True', delimiter=',').csv("data.csv")
    dataset.show(70)

    spark.stop()