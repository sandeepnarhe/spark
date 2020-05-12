package sparktutorials

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType

object SQLDataFrame {

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("CSV Reader")
      .master("local")
      .getOrCreate();

    val ownSchema = StructType(
      StructField("cust_id", IntegerType, true) ::
        StructField("cust_name", StringType, true) ::
        StructField("salary", LongType, true) :: Nil);

    val customerDFWithSchema = spark.read
      .schema(ownSchema)
      .option("header", true)
      .csv("src/test/resources/dataset/customer.csv");

    //Using Temp Table
    customerDFWithSchema.createOrReplaceTempView("Customer_View");
    val customerSQLDF = spark.sql("select * from Customer_View where salary>120");
    customerSQLDF.show();

  }

}