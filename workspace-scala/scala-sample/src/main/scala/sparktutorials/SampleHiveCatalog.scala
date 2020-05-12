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


object SampleHiveCatalog  {

  def main(args: Array[String]) {

    val warehouseLocation = "hdfs://localhost:9000/user/hive/warehouse"

    val spark = SparkSession.builder()
      .appName("DataSet Reader")
      .master("local")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate();
    
    val catalog = spark.catalog;
    
    println(s"current database " + catalog.currentDatabase);
    
  
    
    catalog.setCurrentDatabase("narhedb")
    
    println(s"current database " + catalog.currentDatabase);
    
  }
}