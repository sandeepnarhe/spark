from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("SampleApp").getOrCreate()

emp = spark.read.csv("emp.csv").toDF("id", "name", "company")
emp.show()


empdatamyql = spark.read.format("jdbc").option("url", "jdbc:mysql://172.42.42.100/employee").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "emp").option("user", "temp").option("password", "temp").load()

empdatamyql.show()

empdatamyql.createTempView('mysql_temptable')
spark.sql('select company, count(*) from mysql_temptable group by company').show()
