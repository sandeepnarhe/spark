package sparktutorials


import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random
//import org.apache.spark.sql.functions.{col, from_json, explode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object SampleKafka {
  
  def main(args: Array[String]): Unit = { println("Real-Time Data Pipeline Started ...")

    // Code Block 1 Starts Here
    val kafka_topic_name = "msgtopic"
    val kafka_bootstrap_servers = "172.42.42.101:9092"
    
     // Cassandra Cluster Details
    val cassandra_connection_host = "172.42.42.101"
    val cassandra_connection_port = "9042"
    val cassandra_keyspace_name = "trans_ks"
    val cassandra_table_name = "trans_message_detail_tbl"


    val mongodb_host_name = "34.70.106.130"
    val mongodb_port_no = "27017"
    val mongodb_user_name = "admin"
    val mongodb_password = "admin"
    val mongodb_database_name = "meetup_rsvp_db"
    val mongodb_collection_name = "meetup_rsvp_message_detail_tbl"
    // Code Block 1 Ends Here

    // Code Block 2 Starts Here
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Real-Time Data Pipeline")    
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
    // Code Block 2 Ends Here

    // Code Block 3 Starts Here
    // eCommerce/Retail Message Data from Kafka
    val transaction_detail_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
      .option("subscribe", kafka_topic_name)
      .option("startingOffsets", "latest")
      .load()

    println("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()
    // Code Block 3 Ends Here
    
    // Code Block 4 Starts Here
    val transaction_detail_schema = StructType(Array(
      StructField("results", ArrayType(StructType(Array(
        StructField("user", StructType(Array(
          StructField("gender", StringType),
          StructField("name", StructType(Array(
            StructField("title", StringType),
            StructField("first", StringType),
            StructField("last", StringType)
          ))),
          StructField("location", StructType(Array(
            StructField("street", StringType),
            StructField("city", StringType),
            StructField("state", StringType),
            StructField("zip", IntegerType)
          ))),
          StructField("email", StringType),
          StructField("username", StringType),
          StructField("password", StringType),
          StructField("salt", StringType),
          StructField("md5", StringType),
          StructField("sha1", StringType),
          StructField("sha256", StringType),
          StructField("registered", IntegerType),
          StructField("dob", IntegerType),
          StructField("phone", StringType),
          StructField("cell", StringType),
          StructField("PPS", StringType),
          StructField("picture", StructType(Array(
            StructField("large", StringType),
            StructField("medium", StringType),
            StructField("thumbnail", StringType)
          )))
        )))
      )), true)),
      StructField("nationality", StringType),
      StructField("seed", StringType),
      StructField("version", StringType),
      StructField("tran_detail", StructType(Array(
        StructField("tran_card_type", ArrayType(StringType)),
        StructField("product_id", StringType),
        StructField("tran_amount", DoubleType)
      )))
    ))    
    
    
    val transaction_detail_df_1 = transaction_detail_df.selectExpr("CAST(value AS STRING)")
    val transaction_detail_df_2 = transaction_detail_df_1.select(from_json(col("value"), transaction_detail_schema).as("message_detail"))
    println("Printing Schema for - transaction_detail_df_2 " ) 
    transaction_detail_df_2.printSchema()
     
    val transaction_detail_df_3 = transaction_detail_df_2.select("message_detail.*")
    println("Printing Schema for - transaction_detail_df_3 " ) 
    transaction_detail_df_3.printSchema()
     
    val transaction_detail_df_4 = transaction_detail_df_3.select(
                                                                explode(col("results.user")).as("user"),
                                                                col("nationality"),
                                                                col("seed"),
                                                                col("version"),
                                                                col("tran_detail.tran_card_type").alias("tran_card_type"),
                                                                col("tran_detail.product_id").alias("product_id"),
                                                                col("tran_detail.tran_amount").alias("tran_amount")
                                                              )
                                                              
     println("Printing Schema for - transaction_detail_df_4 " ) 
    transaction_detail_df_4.printSchema()       
    
    
     val transaction_detail_df_5 = transaction_detail_df_4.select(
      col("user.gender"),
      col("user.name.title"),
      col("user.name.first"),
      col("user.name.last"),
      col("user.location.street"),
      col("user.location.city"),
      col("user.location.state"),
      col("user.location.zip"),
      col("user.email"),
      col("user.username"),
      col("user.password"),
      col("user.salt"),
      col("user.md5"),
      col("user.sha1"),
      col("user.sha256"),
      col("user.registered"),
      col("user.dob"),
      col("user.phone"),
      col("user.cell"),
      col("user.PPS"),
      col("user.picture.large"),
      col("user.picture.medium"),
      col("user.picture.thumbnail"),
      col("nationality"),
      col("seed"),
      col("version"),
      col("tran_card_type"),
      col("product_id"),
      col("tran_amount")
    )
    
    println("Printing Schema for - transaction_detail_df_5 " ) 
    transaction_detail_df_5.printSchema()       
    
    
     val getRandomCardType = udf ((array: Seq[String]) => {
      array(Random.nextInt(array.size))
    })

    val transaction_detail_df_6 = transaction_detail_df_5.select(
      col("gender"),
      col("title"),
      col("first").alias("first_name"),
      col("last").alias("last_name"),
      col("street"),
      col("city"),
      col("state"),
      col("zip"),
      col("email"),
      concat(col("username"), round(rand() * 1000, 0).cast(IntegerType)).alias("user_id"),
      col("password"),
      col("salt"),
      col("md5"),
      col("sha1"),
      col("sha256"),
      col("registered"),
      col("dob"),
      col("phone"),
      col("cell"),
      col("PPS"),
      col("large"),
      col("medium"),
      col("thumbnail"),
      col("nationality"),
      col("seed"),
      col("version"),
      getRandomCardType(col("tran_card_type")).alias("tran_card_type"),
      concat(col("product_id"), round(rand() * 100, 0).cast(IntegerType)).alias("product_id"),
      round(rand() * col("tran_amount"), 2).alias("tran_amount")
    )

    val transaction_detail_df_7 = transaction_detail_df_6.withColumn("tran_date",
      from_unixtime(col("registered"), "yyyy-MM-dd HH:mm:ss"))
      
      
       println("Printing Schema for - transaction_detail_df_7 " ) 
    transaction_detail_df_7.printSchema()  
    
    
     val transaction_detail_df_8 = transaction_detail_df_7.select(
      col("user_id"),
      col("first_name"),
      col("last_name"),
      col("gender"),
      col("city"),
      col("state"),
      col("zip"),
      col("email"),
      col("nationality"),
      col("tran_card_type"),
      col("tran_date"),
      col("product_id"),
      col("tran_amount"))
      
      
      
  
     
  /*   transaction_detail_df_8.writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("update")
     .foreachBatch { (batchDF, _) => 
      
        batchDF.write
          .format("org.apache.spark.sql.cassandra")
          .mode("append")
          .option("spark.cassandra.connection.host", cassandra_connection_host)
          .option("spark.cassandra.connection.port", cassandra_connection_port)
          .option("keyspace", cassandra_keyspace_name)
          .option("table", cassandra_table_name)
          .save()
      }.start()*/
 
      
         // Write raw data into HDFS
    transaction_detail_df_7.writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("json")
      .option("path", "/data/json/trans_detail_raw_data")
      .option("checkpointLocation", "/data/checkpoint/trans_detail_raw_data")
      .start()
    
    // Code Block 4 Starts Here
    // Write final result into console for debugging purpose
    val trans_detail_write_stream = transaction_detail_df_8
      .writeStream   
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .outputMode("update")                           
      .option("truncate", "false")
      .format("console")
      .start()
    // Code Block 4 Ends Here

    trans_detail_write_stream.awaitTermination()

    println("Real-Time Data Pipeline Completed.")
    }
  
}