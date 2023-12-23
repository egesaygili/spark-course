import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
#conf.set("spark.local.dir", "/spark/path/to/temporary/directory") # this is needed so that we won't have ERROR ShutdownHookManager
sc = SparkContext(conf = conf)

input = sc.textFile("file:///home/egesaygili/SparkCourse/Book.txt" )
words = input.flatMap(normalizeWords)

# turn word in to (word, 1) and then for each for add up the 1's
# the result is (word, word count)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

# turn (word, word count) into (word count, word) and sort by word count
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
