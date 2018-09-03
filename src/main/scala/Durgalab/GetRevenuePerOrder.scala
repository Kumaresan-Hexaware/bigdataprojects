package Durgalab

import org.apache.spark.{SparkConf,SparkContext}

object GetRevenuePerOrder {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setMaster("local[*]").setAppName("GetRevenue")
    val sc =new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val orderitems =sc.textFile("file:/home/hduser/Durga/data-master/retail_db/order_items/part-00000")
    val revenuePerOrder = orderitems
    .map(x => (x.split(",")(1).toInt,x.split(",")(4).toFloat))
    .reduceByKey(_ + _).map(x =>x._1 + ", " +x._2)  
    .foreach(println)
  
    }
}