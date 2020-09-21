from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[2]").setAppName("Mistra")
sc = SparkContext(conf=conf)

data = [1,2,3,4,5]

distdata=sc.parallelize(data)
print(distdata.collect())

sc.stop()
