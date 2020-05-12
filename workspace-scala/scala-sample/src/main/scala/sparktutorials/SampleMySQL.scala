package sparktutorials

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import java.util.Properties
import org.apache.spark.sql.SaveMode
import java.text.Format

object SampleMySQL {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("DataSet Reader")
      .master("local")
      //.master("yarn")
      .config("spark.testing.memory", "2147480000")
      .config("spark.driver.memory", "571859200")
      .getOrCreate();

    val url = "jdbc:mysql://172.42.42.100:3306"
    val table = "narhedb.employee"
    val properties = new Properties();

    properties.setProperty("user", "hduser");
    properties.setProperty("password", "hduser");
    Class.forName("com.mysql.jdbc.Driver");

    val empDF = spark.read.jdbc(url, table, properties);
    empDF.printSchema();
    empDF.show();
    empDF.select("company").groupBy("company").count().show();

    val query = "select company, count(*) as companies from narhedb.employee group by company";
    val empDFFiltered = spark.read.jdbc(url, s"($query) as companies", properties);
    
    empDFFiltered.show();
    
    val empDFCSV = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/test/resources/dataset/employee.csv")
    
    val table_tosave = "narhedb.employee";
    
    empDFCSV.write.mode(SaveMode.Append).jdbc(url, table_tosave, properties);
    empDFCSV.write.format("csv").save("/user/hduser/dataset/employee-db.csv");

  }

}