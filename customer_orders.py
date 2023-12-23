from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
#conf.set("spark.local.dir", "/path/to/temporary/directory") # this is needed so that we won't have ERROR ShutdownHookManager
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerId = int(fields[0])
    moneySpent = float(fields[2])
    return (customerId, moneySpent)

lines = sc.textFile("file:///home/egesaygili/SparkCourse/customer-orders.csv")
parsedLines = lines.map(parseLine)

moneySpentByCustomer = parsedLines.reduceByKey(lambda x,y: x+y)

sorted_midstep = moneySpentByCustomer.map(lambda x: (x[1], x[0])).sortByKey()
sorted = sorted_midstep.map(lambda x: (x[1], x[0])).collect()


for result in sorted:
    #print(str(result[0]), "{:.2f}".format(result[1]))
    print(result)