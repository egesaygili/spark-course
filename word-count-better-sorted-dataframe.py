from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of my book into a dataframe
inputDF = spark.read.text("file:///home/egesaygili/SparkCourse/Book.txt")
#inputDF.show()

# Split using a regular expression that extracts words
# the column will be called "value" by default
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word")) # alias is in select function
wordsWithoutEmptyString = words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts, the column name will ve "count" by default
wordCountsSorted = wordCounts.sort("count")

# Show the results.
wordCountsSorted.show(wordCountsSorted.count()) # this way you print every single row there is

spark.stop()