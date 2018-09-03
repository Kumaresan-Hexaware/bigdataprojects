package spark.test

import org.apache.spark.SparkConf
import  org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
//import org.json4s.native.JsonFormats.parse
import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import com.datastax.spark.connector._
//import com.datastax.bdp.spark.writer.BulkTableWriter._
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.Row
import org.apache.spark.sql
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark.rdd.EsSpark

object randomuser {  
  //case class df1 (results:String)
 def main(args:Array[String])
  {
    //val strkafkatopic = args(0)
    val sparkConf = new SparkConf().setAppName("kafkalab").setMaster("local[*]").set("spark.sql.crossJoin.enabled", "true")
    //val sparkConf = new SparkConf().setAppName("kafkalab").set("spark.sql.crossJoin.enabled", "true")
        val sparkcontext = new SparkContext(sparkConf)
        val sqlContext = new SQLContext(sparkcontext)
        import sqlContext.implicits._
        sparkcontext.setLogLevel("ERROR")
        val ssc = new StreamingContext(sparkcontext, Seconds(5))
        ssc.checkpoint("file:///tmp/checkpointdir")
        val kafkaParams = Map[String, Object](
          "bootstrap.servers" -> "localhost:9092",
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> "kafkatk5",
          "auto.offset.reset" -> "latest"
          )

        //val topics = Array(strkafkatopic)
          val topics = Array("tk3")
        val stream = KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](topics, kafkaParams)
        )

        val kafkastream = stream.map(record => (record.key, record.value))
        kafkastream.print()
        val inputStream = kafkastream.map(rec => rec._2);
        inputStream.foreachRDD(rdd=>
    {   
      val jsonrdd = rdd.filter(_.contains("results"))
        
      if(!jsonrdd.isEmpty)
      {
              //jsonrdd.foreach(println)
              val df = sqlContext.read.json(jsonrdd)
              //df.show
              val df1 = df.select(explode($"results"))
              val df2 = df1.select($"col.cell" as "cell",$"col.dob" as "dob",$"col.email" as "email",$"col.gender" as "gender")
              val df3 = df1.select($"col.id.name" as "name",$"col.id.value" as "value")
              val rdd4 = df2.join(df3).rdd
              rdd4.saveAsTextFile("hdfs://localhost:54310/user/hduser/randomdata.txt")
              println("rdd4 is stored to HDFS")
              //EsSpark.saveJsonToEs(rdd4,"http://localhost:9200/random")
              //val dfes = df2.join(df3)
              //dfes.saveToEs("random/user")
              //df1.write.json("hdfs://localhost:54310/user/hduser/ramdomdata.txt")
              //df1.write.text("hdfs://localhost:54310/user/hduser/ramdomdata.txt")
              //df1.saveAsTextFile()
              //df1.printSchema()
     }
    })    
    ssc.start()
    ssc.awaitTermination()
  }    
 }