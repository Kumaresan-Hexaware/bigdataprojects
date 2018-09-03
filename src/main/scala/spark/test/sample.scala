package spark.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


object sample {
  
  def main(args:Array[String])
  {
    val conf = new SparkConf().setMaster("local[*]").setAppName("lab1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd = sc.textFile("file:/home/hduser/sparkdata/custdata.txt")
    rdd.flatMap(x => x.split(",")).map(x => (x,1))
    .reduceByKey((x,y) => (x + y))
    .foreach(println)   
  }
  
}