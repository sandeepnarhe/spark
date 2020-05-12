package sparktutorials

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType

object DataframeCSV {

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("CSV Reader")
      .master("local")
      .getOrCreate();
    
    
    val rdd = spark.sparkContext.parallelize(Array(1,2,3,4));
    rdd.collect().foreach(println);

    val properties = Map("header" -> "true", "inferSchema" -> "true");

    val customerDF = spark.read
      .options(properties)
      .csv("src/test/resources/dataset/customer.csv");

    customerDF.printSchema();
    customerDF.show()
    
    val ownSchema = StructType(
        StructField("cust_id",IntegerType,true)::
        StructField("cust_name",StringType,true)::
        StructField("salary",LongType,true)::Nil
       );
   
     val customerDFWithSchema = spark.read
      .schema(ownSchema)
      .option("header", true)
      .csv("src/test/resources/dataset/customer.csv");
    
    //Print Schema 
    customerDFWithSchema.printSchema()
    
    //Get Just Columns
    println(customerDFWithSchema.columns.mkString(","));
       
    //Describe Stats
    customerDFWithSchema.describe("salary","cust_id").show();
    
    //Print Top5 records
    customerDFWithSchema.head(2).foreach(println);
    customerDFWithSchema.show(2);
    
    //Show Only selected Records
    customerDFWithSchema.select("cust_name").show();
    customerDFWithSchema.select("cust_name").where("salary > 100").show();
    customerDFWithSchema.select("cust_name").where("cust_name='rohan'").show();
    customerDFWithSchema.select("cust_name","salary").groupBy("cust_name").sum().show();
    
    
    //Using Temp Table
    customerDFWithSchema.createOrReplaceTempView("Customer_View");
    val customerSQLDF = spark.sql("select * from Customer_View where salary>120" );
    customerSQLDF.show();
    
    
    
  }

}