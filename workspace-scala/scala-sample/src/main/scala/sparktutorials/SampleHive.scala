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

object SampleHive {

  def main(args: Array[String]) {

    val warehouseLocation = "hdfs://localhost:9000/user/hive/warehouse"

    val spark = SparkSession.builder()
      .appName("DataSet Reader")
      .master("yarn")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate();

    val empDFCSV = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/user/hduser/src/test/resources/dataset/employee.csv");

    empDFCSV.write
      .option("path", "/")
      .mode(SaveMode.Overwrite)
      .saveAsTable("employee_new_external");

   /* import spark.implicits._
    import spark.sql
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    sql("INSERT INTO SRC VALUES(1,'SANDEEP')");
    sql("LOAD DATA LOCAL INPATH '/home/hduser/hive.csv' INTO TABLE src")
    sql("SELECT * FROM src").show()*/

  }

}