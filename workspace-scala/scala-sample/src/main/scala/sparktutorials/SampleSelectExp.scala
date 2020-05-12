package sparktutorials

import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.{ DataFrame, SparkSession }

import scala.util.Random
//import org.apache.spark.sql.functions.{col, from_json, explode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import javassist.expr.Cast

object SampleSelectExp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val simpleData = Seq(
      Row("James", 34, "2006-01-01", "true", "M", 3000.60),
      Row("Michael", 33, "1980-01-10", "true", "F", 3300.80),
      Row("Robert", 37, "06-01-1992", "false", "M", 5000.50))

    val simpleSchema = StructType(Array(
      StructField("firstName", StringType, true),
      StructField("age", IntegerType, true),
      StructField("jobStartDate", StringType, true),
      StructField("isGraduated", StringType, true),
      StructField("gender", StringType, true),
      StructField("salary", DoubleType, true)))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(simpleData), simpleSchema)
    df.printSchema()
    df.show(false)

    //Change column type using withColumn and cast
    val df2 = df.withColumn("age", col("age").cast(StringType))
      .withColumn("isGraduated", col("isGraduated").cast(BooleanType))
      .withColumn("jobStartDate", col("jobStartDate").cast(DateType))
    df2.printSchema()

    //Change Column type using selectExpr
    val df3 = df2.selectExpr("cast(age as int) age")
    df3.printSchema();

    df.createOrReplaceTempView("CastExample")
    val df4 = spark.sql("SELECT STRING(age),BOOLEAN(isGraduated), DATE(jobStartDate) from CastExample")
    df4.printSchema()
    df4.show(false)

  }
}