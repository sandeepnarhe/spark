package sparktutorials

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType

case class Customer(customer_id: Integer, customer_name: String, invoice: Double);
case class CustomerShort(customer_id: Integer, customer_name: String);

object SampleDataSet {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("DataSet Reader")
      .master("local")
      .getOrCreate();

    import spark.implicits._
    
    val customerDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/test/resources/dataset/customer.csv")
      .as[Customer];
    
   customerDF.show();
   
   //Filter
   val filteredCustomer = customerDF.filter(customer => customer.customer_name == "rohan");
   
   filteredCustomer.show();
   
   //Select
   val selectedCustomer = customerDF.select("customer_id", "customer_name")
                 .as[CustomerShort];
   selectedCustomer.show();
  

  }

}