from pyspark import SparkConf, SparkContext

if _name_ == '_main_':
    conf = SparkConf().setMaster("local[2]").setAppName("Map")
    sc = SparkContext(conf=conf)


    def my_map():
        data = [1, 2, 3, 4, 5]
        rdd1 = sc.parallelize(data)
        rdd2 = rdd1.map(lambda x: x * 2)

        print(rdd2.collect())


    def my_map2():
        data = sc.parallelize(["dog", "tiger", "lion", "cat", "AAA"])
        b = data.map(lambda x: (x, 1))
        print(b.collect())


    def my_filter():
        data = [1, 2, 3, 4, 5]
        rdd1 = sc.parallelize(data)
        mapRdd= rdd1.map(lambda x:x*2)
        filterRdd = mapRdd.filter(lambda x:x>5)
        print(filterRdd.collect())


    def my_flatMap():
        data = ["hello spark","hello world"]
        rdd = sc.parallelize(data)
        print(rdd.flatMap(lambda line:line.split(" ")).collect())

    def my_groupBy():
        data = ["hello spark","hello world"]
        rdd = sc.parallelize(data)
        print(rdd.flatMap(lambda line:line.split(" ")).map(lambda x:(x,1))
              .groupByKey().map(lambda x:{x[0]:list(x[1])}).collect())
    def my_reduceByKey():
        data = ["hello spark","hello world"]
        rdd = sc.parallelize(data)
        print(rdd.flatMap(lambda line:line.split(" ")).map(lambda x:(x,1))
              .reduceByKey(lambda a,b:a+b).collect())
    def my_sort():
        data = ["hello spark","hello world"]
        rdd = sc.parallelize(data)
        print(rdd.flatMap(lambda line:line.split(" ")).map(lambda x:(x,1))
              .reduceByKey(lambda a,b:a+b).map(lambda x:(x[1],x[0])).sortByKey().collect())

    def my_union():
        a = sc.parallelize([1,2,3,4])
        b = sc.parallelize([3,4,5])
        print(a.union(b).collect())

    def my_distinct():
        a = sc.parallelize([1,2,3,4])
        b = sc.parallelize([3,4,5])
        print(a.union(b).distinct().collect())

    my_map()
    my_map2()
    my_filter()
    my_flatMap()
    my_groupBy()
    my_reduceByKey()
    my_sort()
    my_union()
    my_distinct()



    sc.stop()
