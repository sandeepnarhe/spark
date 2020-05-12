
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions._

object SampleStructureStream {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Sample Structured Stream")
      // .master("spark://hmaster.example.com:7077")
      .master("local[2]")
      .getOrCreate();

    // Cassandra Cluster Details
    val cassandra_connection_host = "10.0.0.15"
    val cassandra_connection_port = "9042"
    val cassandra_keyspace_name = "trans_ks"
    val cassandra_table_name = "emp_tbl"

    // MongoDB Cluster Details
    val mongodb_host_name = "10.0.0.15"
    val mongodb_port_no = "27017"
    val mongodb_user_name = "myTester"
    val mongodb_password = "myTester"
    val mongodb_database_name = "test"

    spark.sparkContext.setLogLevel("INFO")

    val schema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
      .add("company", StringType)
      .add("salary", IntegerType)
      .add("age", IntegerType);

    val streamData = spark.readStream
      .schema(schema)
      .option("maxFilesPerTrigger", "1")
      .csv("src/test/resources/data")
    //  .csv("/home/hduser/data")

    /* val filtered_data = streamData
    .filter("company = 'infy'")
    .where2("salary>100")
    .drop("age")
    .select("name","id")
    .writeStream
    .queryName("infy-query")
    .format("console")
    .outputMode(OutputMode.Update())
    .start();

    val groupByData = streamData
    .groupBy("company")
    .agg(sum("salary"))
    .writeStream
    .queryName("infy-query-salary")
    .format("console")
    .outputMode(OutputMode.Update())
    .start();*/

    val filtered_data = streamData
      .filter("company = 'infy'")
      .drop("age")
      .select("name", "id")
      .writeStream
      .queryName("infy-query-1")
      //.format("console")
      .outputMode(OutputMode.Update())
      .foreachBatch {
        (batchDF: DataFrame, batchId: Long) =>
          batchDF.write
            .format("org.apache.spark.sql.cassandra")
            .mode("append")
            .option("spark.cassandra.connection.host", cassandra_connection_host)
            .option("spark.cassandra.connection.port", cassandra_connection_port)
            .option("keyspace", cassandra_keyspace_name)
            .option("table", cassandra_table_name)
            .save()
      }
      .start();

    val mongodb_collection_name = "emp_tbl"
    val spark_mongodb_output_uri_1 = "mongodb://" + mongodb_user_name + ":" + mongodb_password + "@" + mongodb_host_name + ":" + mongodb_port_no + "/" + mongodb_database_name + "." + mongodb_collection_name
    println("Printing spark_mongodb_output_uri: " + spark_mongodb_output_uri_1)

    val filtered_data_mongo = streamData
      .filter("company = 'infy'")
      .drop("age")
      .select("name", "id")
      .writeStream
      .queryName("infy-query")
      //.format("console")
      .outputMode(OutputMode.Update())
      .foreachBatch {
        (batchDF: DataFrame, batchId: Long) =>

          batchDF.write
           // .format("mongo")
              .format("com.mongodb.spark.sql.DefaultSource")
            .mode("overwrite")
            .option("uri", spark_mongodb_output_uri_1)
            .option("database", mongodb_database_name)
            .option("collection", mongodb_collection_name)
            .save()
      }.start()

    filtered_data.awaitTermination();

  }
}