from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///home/egesaygili/SparkCourse/Marvel+Names")

lines = spark.read.text("file:///home/egesaygili/SparkCourse/Marvel+Graph")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
mostObscureCount = connections.agg(func.min("connections")).first()[0] # since there's no group by, just returns the minimum value


charactersWithOneConnection = connections.filter(func.col("connections") == 1).select(connections.id, connections.connections)

charactersWithOneConnectionWithNames = charactersWithOneConnection.join(names, "id")

print("Characters with 1 connection:")
charactersWithOneConnectionWithNames.show(charactersWithOneConnectionWithNames.count())

charactersWithMostObscureCount = connections.filter(func.col("connections") == mostObscureCount).select(connections.id, connections.connections)

charactersWithMostObscureCountWithNames = charactersWithMostObscureCount.join(names, "id")

print("Characters with the least connections:")
charactersWithMostObscureCountWithNames.show(charactersWithMostObscureCountWithNames.count())

