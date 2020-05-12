
package sparktutorials

import org.apache.spark.sql.SparkSession;

object SampleHDFS {  

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("SimpleHDFS")
      .master("yarn")
      .config("spark.hadoop.fs.defaultFS", "hdfs://hmaster.example.com:9000")
      .config("spark.hadoop.yarn.resourcemanager.address", "hmaster.example.com:8032")
      .config("spark.hadoop.yarn.resourcemanager.hostname", "hmaster.example.com")
      .config("spark.yarn.jars", "hdfs://hmaster.example.com:9000/user/hduser/jars/*.jar")
      .config("spark.cores.max", "1")
      .config("spark.executor.instances", "1")
      .config("spark.executor.memory", "500m")
      .config("spark.executor.cores", "1")
      .config("spark.shuffle.service.enabled", "false")
      .config("spark.dynamicAllocation.enabled", "false")
      .config("spark.testing.memory", "2147480000")
      .config("spark.driver.memory", "571859200")
      .config("spark.hadoop.yarn.application.classpath", "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*")
      .getOrCreate();

     val stockDF = spark.read
      .option("header", "true")
      .csv("/user/sande/data-dump/employee.txt");
    
    
    stockDF.show()

    print("Spark Sample Job on yarn Cluster")
  }

} 