from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
#conf.set("spark.local.dir", "/path/to/temporary/directory") # this is needed so that we won't have ERROR ShutdownHookManager
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///home/egesaygili/SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMAX" in x[1]) #filter with a boolean expression
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: max(x,y)) # here, x and y corresponds to values from different rows
results = minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
