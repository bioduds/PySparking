from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PySpark Intro").getOrCreate()

## Create a DataFrame for a simple list
data = [("Alice", 34),("Bob", 459),("Cathy",29)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

df.show()

spark.stop()