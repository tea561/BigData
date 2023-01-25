from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == '__main__':
    input_file = "train.csv"
    spark = SparkSession.builder.appName('data_transformation').master('spark://localhost:7077').getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    dataset = spark.read.option("inferSchema", True).option("header", True).csv(input_file)

    dataset.printSchema()
    dataset.show(10)
    
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
    dataset = dataset.withColumn("trip_duration", F.size(F.col("array_of_coordinates")) * 7500 )

    dataset = dataset.withColumn("end_time", dataset["start_time"] + dataset["trip_duration"])
    dataset = dataset.drop("polyline")
    dataset = dataset.drop("coordinates")
    dataset = dataset.drop("array_of_coordinates")

    dataset.show(10)

    spark.stop()


