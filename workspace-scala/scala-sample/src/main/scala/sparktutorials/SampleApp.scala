package sparktutorials

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object SampleApp {
  
  /**
   * spark-submit --driver-memory 500mb --executor-memory 700mb  --class sparktutorials.SampleApp --master yarn --deploy-mode cluster  ~/spark-job.jar 1
		 spark-submit  --class sparktutorials.SampleApp --master yarn --deploy-mode client  ~/spark-job.jar 1
   */
  
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Cloudera Sample Job")
       .master("spark://172.42.42.100:7077")
      //.master("spark://172.42.42.100:7077")
      //.master("local")
      //.master("yarn")
      .config("spark.testing.memory", "2147480000")
      .config("spark.driver.memory", "571859200")

      .getOrCreate()
      
     spark.sparkContext.setLogLevel("INFO")
     
     val stockDF = spark.read
      .option("header", "true")
      .csv("/user/sande/data-dump/employee.txt");
    
    
    stockDF.show()
    
    print("This is Sandeep Narhe - Infosys");
  }
  
  
 
  
}  