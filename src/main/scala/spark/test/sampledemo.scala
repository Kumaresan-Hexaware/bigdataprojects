package spark.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object sampledemo {
  
  def main (args: Array[String]){
    val conf = new SparkConf().setMaster("local[*]").setAppName("sample")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("file:/home/hduser/sparkdata/custdata.txt")
    //.foreach(println)
    //rdd.flatMap(x => x.split(",")).map(x =>(x,1))
    rdd.flatMap(x => x.split(",")).map(x =>(x,1))
    .reduceByKey((x,y)=>(x+y))
    .foreach(println)
    val sqlContext = new SQLContext(sc)
    val csvdf = sqlContext.read.option("header","true").option("delimiter",",")
    .csv("file:///home/hduser/sparkdata/usdata.csv")
    csvdf.show()
    csvdf.createOrReplaceTempView("usdata")
    sqlContext.sql("select * from usdata where state='CA'").show(1000)
    
    
  }
}