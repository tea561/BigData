from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, MinMaxScaler, RobustScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressionModel, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.stat import Correlation
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import os
# import pandas
# import matplotlib.pyplot as plt    

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
    
    columns = ['origin_call', 'call_type_num', 'origin_stand', 'taxi_id', 'start_time', 'start_lat', 'start_lon', 'end_lat', 'end_lon']
    nullable_columns = ['origin_call', 'origin_stand', 'taxi_id', 'start_time', 'start_lat', 'start_lon', 'end_lat', 'end_lon']

    appName = "P3 - Model Training"
    HDFS_INPUT = os.getenv('HDFS_PORTO')
    MODEL_LOCATION = os.getenv('MODEL_LOCATION')
    SCALER_LOCATION = os.getenv('SCALER_LOCATION')
    INDEXER_LOCATION=os.getenv('INDEXER_LOCATION')
    spark = SparkSession.builder.appName('P3 - Model Training').master("spark://spark-master:7077").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    dataset = spark.read.option("inferSchema", True).option("header", True).csv(HDFS_INPUT)

    for col_name in nullable_columns:
        dataset = dataset.withColumn(col_name, F.when(F.isnull(dataset[col_name]), 0).otherwise(dataset[col_name]).alias(col_name))
    dataset.show()

    dataset = dataset.drop('missing_data')
    dataset = dataset.drop('trip_id')
    dataset = dataset.drop('day_type')
    

    indexer_model = StringIndexer(inputCol = 'call_type', outputCol = 'call_type_num').fit(dataset)
    indexer_model.write().overwrite().save(INDEXER_LOCATION)
    indexed = indexer_model.transform(dataset)

    va = VectorAssembler().setInputCols(columns).setOutputCol('features').setHandleInvalid("skip").transform(indexed)
    
    va.describe().show()
    
    split = va.randomSplit([0.8, 0.2], 3333)
    scaler = RobustScaler().setInputCol('features').setOutputCol('scaled_features')
    scaler_model = scaler.fit(split[0])
    scaler_model.write().overwrite().save(SCALER_LOCATION)
    train = scaler_model.transform(split[0])
    test = scaler_model.transform(split[1])

    gbt = GBTRegressor(featuresCol='scaled_features', labelCol='trip_duration', maxIter=10)
    
    pipeline = Pipeline(stages=[gbt])

    paramGrid = ParamGridBuilder()\
        .addGrid(gbt.maxBins, [100, 200])\
        .addGrid(gbt.maxDepth, [2, 4, 10])\
        .build()
    
    evaluator = RegressionEvaluator(labelCol='trip_duration', predictionCol='prediction', metricName='rmse')

    cv = CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(5)

    mp = cv.fit(train)

    prediction = mp.transform(test)
    prediction.show()

    evaluator = RegressionEvaluator(labelCol='trip_duration', predictionCol='prediction', metricName='r2')
    r2 = evaluator.evaluate(prediction)
    evaluator = RegressionEvaluator(labelCol='trip_duration', predictionCol='prediction', metricName='mae')
    mae = evaluator.evaluate(prediction)

    print("GBT Regressor")
    print('R2 ' + str(r2))
    print('MAE ' + str(mae))

    mp.write().overwrite().save(MODEL_LOCATION)

    #dataset.describe().show()

    # dfPlot = dataset.groupBy('call_type').sum("Quantity")
    # x = dfPlot.to_pandas()['call_type'].values.toList()
    # y = dfPlot.to_pandas()['sum(Quantity)'].values.toList()
    # plt.plot(x, y)
    # plt.savefig('plot1.png')

    spark.stop()
