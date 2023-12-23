from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
#conf.set("spark.local.dir", "/spark/path/to/temporary/directory") # this is needed so that we won't have ERROR ShutdownHookManager
sc = SparkContext(conf = conf)

input = sc.textFile("file:///home/egesaygili/SparkCourse/Book.txt") # might as well just pass "Book.txt"
words = input.flatMap(lambda x: x.split()) # flatMap maps to the values in a list, but not line by line
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore') # turns the characters into ascii and ignore the conversion errors
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
