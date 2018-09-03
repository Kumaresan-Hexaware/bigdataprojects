package spark.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import com.datastax.spark.connector._

object usecase {
  case class customer(NAME:String,AGE:Int,GENDER:String,LATTITUDE:Long,LONGITUDE:Long)
  case class activity(id:Int,NAME:String,PLATFORM:String,HOURS:String,DATE:String)
  case class location(LATTITUDE:Long,LONGITUDE:Long,CITY:String)
  def main(args:Array[String])
  {
    val conf = new SparkConf().setMaster("local").setAppName("usecase")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd1 = sc.textFile("file:///home/hduser/Desktop/use case to resolve/spark-scala/customer.txt").map(_.split(",")).map(x =>customer(x(0),x(1).toInt,x(2),x(3).toLong,x(4).toLong))
    rdd1.foreach(println)
    val rdd2 = sc.textFile("file:///home/hduser/Desktop/use case to resolve/spark-scala/activity.txt").map(_.split(",")).map(x =>activity(x(0).toInt,x(1),x(2),x(3),x(4)))
    rdd2.foreach(println)
    val rdd3 = sc.textFile("file:///home/hduser/Desktop/use case to resolve/spark-scala/location.txt").map(_.split(",")).map(x =>location(x(0).toLong,x(1).toLong,x(2)))
    //val rdd = rdd1.join(rdd2).map(x => (x._1,x._2,x._3,x._4,x._5(x._2._1,x._2._3,x._2._4)))
    //import sqlContext.implicits._
    //rdd4.show()
    rdd3.foreach(println)  
    println("success ")
  }
}