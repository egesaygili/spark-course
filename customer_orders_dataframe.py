from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpent").master("local[*]").getOrCreate()

schema = StructType([ \
                     StructField("customerID", IntegerType(), True), \
                     StructField("productID", IntegerType(), True), \
                     StructField("moneySpent", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("file:///home/egesaygili/SparkCourse/customer-orders.csv")
df.printSchema()


df_res = df.groupBy("customerID").agg(func.round(func.sum("moneySpent"), 2).alias("totalSpent")).sort("totalSpent")

df_res.show(df_res.count())

spark.stop()