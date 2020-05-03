from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .enableHiveSupport() \
    .appName("SampleApp").getOrCreate()

df = spark.sql("select * from narhe_db.tb_stock")
df.show()

