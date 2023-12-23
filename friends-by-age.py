from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
#conf.set("spark.local.dir", "/path/to/temporary/directory") # this is needed so that we won't have ERROR ShutdownHookManager
sc = SparkContext(conf = conf)

# to read the lines and convert them into (key, value) tuples/pairs
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("file:///home/egesaygili/SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)
# turn the (key, value) pairs into (key, (value, 1)) i.e. (age, (number of friends, 1))
# sum up the (value, 1) parts of the pair having the same keys
# the result will be (key, (sum of the values, row count)) for each value 
# i.e. (age, (total number of friends for that age, number of people that have the age))

# mapValues passes only the value in (key, value) pair 
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) #x and y corresponds to (value, 1) pairs in different rows
# turn (age, (total number of friends for that age, number of people that have the age)) into
# (age, total number of friends for that age / number of people that have the age)
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)
