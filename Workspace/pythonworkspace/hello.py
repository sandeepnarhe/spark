from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

print("Hello!!")

# sc = SparkContext(master="spark://172.42.42.100:7077", appName="SampleApp")
# spark = SparkContext("local", appName="SampleApp").getOrCreate()
# peopleDF = spark.read.json("people.json")
# peopleDF.printSchema()
# print(spark.textFile("emp.txt"))

spark = SparkSession.builder \
    .master("local") \
    .appName("SampleApp").getOrCreate()

emp = spark.read.csv("emp.csv").toDF("id", "name", "company")
empData = emp.withColumn("id", emp.id.cast(IntegerType()))
empData.printSchema()
empData.show()


emp.select('name').show()

# To write select query, we need to create temp view

empData.createTempView('emp_data_temp_view')
spark.sql("select * from emp_data_temp_view").show()

# Read Data from RDBMS

empdatamyql = spark.read.format("jdbc")\
    .option("url", "jdbc:mysql://172.42.42.100/employee")\
    .option("driver", "com.mysql.jdbc.Driver")\
    .option("dbtable", "emp")\
    .option("user", "temp")\
    .option("password", "temp")\
    .load()

empdatamyql.show()

empdatamyql.createTempView('mysql_temptable')
spark.sql('select company, count(*) from mysql_temptable group by company').show()

empdatamyql.write.csv("mysqldb.csv")



