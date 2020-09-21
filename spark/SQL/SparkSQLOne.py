from pyspark.sql import SparkSession

# SparkSQL 演示
# 参照http://spark.apache.org/docs/2.3.0/sql-programming-guide.html

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Mistra").getOrCreate()
    df = spark.read.json("file:///C:/work/spark-2.4.6-bin-hadoop2.7/examples/src/main/resources/people.json")
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select(df['name'], df['age'] + 1).show()
    df.filter(df['age'] > 21).show()

    # 注册临时视图
    df.createOrReplaceTempView("people")
    result = spark.sql("select * from people")
    result.show()

    spark.stop()
