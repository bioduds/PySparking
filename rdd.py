from pyspark.sql import SparkSession

## Initialize the Spark Session
spark = SparkSession.builder.appName("RDD Basis").getOrCreate()

# Create an RDD from a Python list
data = [1,2,3,4,5]
rdd = spark.sparkContext.parallelize(data)

## Collect and show the data
print( rdd.collect() )

spark.stop()

## if an external source is used, code example, uncomment to use:

''' uncomment this to use
rdd_file = spark.sparkContext.textFile("sample.txt")

# Show the first 5 lines
print( rdd_file.take(5) )
'''