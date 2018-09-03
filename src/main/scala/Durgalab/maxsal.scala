package Durgalab
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql
import org.apache.spark.sql.Dataset;
import javax.validation.constraints.Max

object maxsal {
  case class Dept(dept_name : String, Salary : Int)
  
  def main(args: Array[String]) : Unit ={
  val conf = new SparkConf().setMaster("local[*]").setAppName("Maxsal")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  
   val maxsal = sc.textFile("file:/home/hduser/sparkdata/empsal.txt")
    .mapPartitionsWithIndex((idx,row) => if (idx==0)row.drop(1) else row )
    .map(x =>(x.split(",")(0).toString,x.split(",")(1).toInt))
    
    val maxs = maxsal.reduceByKey(math.max(_,_))
    .foreach(println)
    
    import sqlContext.implicits._
    val maxrdd = sc.textFile("file:/home/hduser/sparkdata/empsal.txt").map(_.split(","))
    val maxrdd1 = maxrdd.map(r=>Dept(r(0),r(1).toInt)).toDS()
    
    //maxrdd1.groupBy($"dept_name").agg(maxrdd1("Salary").alias("max_solution")).show()
        
  }
  }