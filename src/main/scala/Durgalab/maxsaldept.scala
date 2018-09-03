package Durgalab
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

object maxsaldept {
  def main(args: Array[String]) {
  
  val conf = new SparkConf().setMaster("local[*]").setAppName("maxsal")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  
  val maxsal = sc.textFile("file:/home/hduser/sparkdata/empsal1.txt")
  .map(x=>(x.split(" "))).map(x=>((x(2)),(x(1),x(4).toDouble))).groupByKey
      for(i <- maxsal)
      {
         println(i._1,i._2.filter(x=>x._2==i._2.map(x=>x._2).max))
      }
  }  
  
}