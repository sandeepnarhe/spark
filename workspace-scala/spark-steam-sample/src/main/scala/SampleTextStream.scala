
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SampleTextStream {
    def main(args: Array[String]): Unit = {
      
      val conf = new SparkConf().setMaster("local[2]")
        .setAppName("Streaming Example");
        
      val ssc= new StreamingContext(conf, Seconds(5));
      
      val line = ssc.socketTextStream("172.42.42.101", 9999)
    //  line.print()
      val words = line.flatMap(_.split(" "));
      
      val pairs = words.map(words => (words,1))
      pairs.print()
      
      val wordConuts = pairs.reduceByKey(_+_);
      wordConuts.print();
      
      ssc.start();
      ssc.awaitTermination();
    }    
}