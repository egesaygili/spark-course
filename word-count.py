from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
conf.set("spark.local.dir", "/spark/path/to/temporary/directory") # this is needed so that we won't have ERROR ShutdownHookManager
sc = SparkContext(conf = conf)

input = sc.textFile("file:///Sparkcourse(TamingBigData)/Book.txt")
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
