package sparktutorials

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SampleSparkStream {
 
  def main(args: Array[String]): Unit = {
     
      val conf = new SparkConf().setMaster("local[2]")
        .setAppName("Streaming Example");
        
      val ssc= new StreamingContext(conf, Seconds(5));
      
      val line = ssc.socketTextStream("172.42.42.101", 9999)
      
      val words = line.flatMap(_.split(" "));
      print(words)
     
  }
}