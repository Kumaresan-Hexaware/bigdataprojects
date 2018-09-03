package spark.test

import org.apache.spark._
object writefile {
  
  def main(args:Array[String])
  {
     val conf = new SparkConf().setAppName("usecase8").setMaster("local")                                                                                      
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd = sc.textFile("hdfs://localhost:54310/user/hduser/empinfo").map(_.split(",")).map(x=>(x(0),1))
    val rdd1 = rdd.keys
    val rdd2 = rdd.values
    //rdd.saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkoutput6")    
    rdd1.foreach(println)
  }
}