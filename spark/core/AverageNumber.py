import sys

# 统计平均数
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf()
    sc = SparkContext(conf = conf)
    if len(sys.argv) !=2:
        print("Usage: wordcount <input>", file=sys.stderr)
        sys.exit(-1)

    conf  =SparkConf()
    sc = SparkContext(conf = conf)

    ageData = sc.textFile(sys.argv[1]).map(lambda x:x.split(" ")[1])
    totalAge = ageData.map(lambda age:int(age)).reduce(lambda a,b:a+b)
    counts = ageData.count()
    avgAge = totalAge/counts
    print(counts)
    print(totalAge)
    print(avgAge)

    sc.stop()
