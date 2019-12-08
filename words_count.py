from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext


def startCountWords():
    sc = SparkContext("local[2]", "WordsCount")
    ssc = StreamingContext(sc, 15)  # read every 5 seconds
    lines = ssc.socketTextStream("localhost", 9999)
    lines.pprint()
    words = lines.flatMap(lambda line: line.split(" "))
    pairs = words.map(lambda word: (word, 1))
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)
    wordCounts.pprint()
    ssc.start()  # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate


def startReverseWords():
    sc = SparkContext("local[2]", "ReverseCount")
    ssc = StreamingContext(sc, 15)  # read every 15 seconds
    lines = ssc.socketTextStream("localhost", 9999)
    lines.pprint()
    words = lines.flatMap(lambda line: line.split(" ")).map(lambda word: word[::-1])  # split lines and reverse
    words.pprint()
    ssc.start()  # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate


def startReadLogFile():
    sc = SparkContext("local[2]", "FileAnalyze")
    textFile = sc.textFile("/home/madi/PycharmProjects/spark_python/data")
    words = textFile.flatMap(lambda line: line.split(" "))
    word_pairs = words.map(lambda word: (word, 1))
    wordCounts = word_pairs.reduceByKey(lambda a, b: a + b)
    lines = wordCounts.collect()
    for line in lines:
        print(line)
    wordCounts.saveAsTextFile("/home/madi/PycharmProjects/spark_python/result")


if __name__ == "__main__":
    #  startCountWords()
    #  startReverseWords()
    startReadLogFile()
