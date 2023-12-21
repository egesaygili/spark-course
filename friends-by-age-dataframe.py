from pyspark.sql import SparkSession, Row, functions as func


spark = SparkSession.builder.appName("FreindsByAge").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///home/egesaygili/SparkCourse/fakefriends-header.csv")

# Get average number of friends by age
people.groupBy("age").avg("friends").show()

# Get average number of friends by age sorted by age
people.groupBy("age").avg("friends").sort("age").show()

# Same thing but formatted nicely
people.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

# Added custom column name
people.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friends_avg")).sort("age").show()

spark.stop()