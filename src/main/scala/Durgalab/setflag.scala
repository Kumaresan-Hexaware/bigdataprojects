package Durgalab
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object setflag {
  def main(args: Array[String]){
    
    val Conf = new SparkConf()
    val sc = new SparkContext(Conf)
    
   val lists= List(("00111","00111651","4444","PY","MA"),
                   ("00111","00111651","4444","XX","MA"),
                   ("00112","00112P11","5555","TA","MA"))

   val grouped = lists.groupBy{case(a,b,c,d,e) => (a,b,c)}
   val indexed = grouped.mapValues(
               _.zipWithIndex
                .map {case ((a,b,c,d,e), idx) => (a,b,c + (idx+1).toString,d,e)})
   val unwrapped = indexed.flatMap(_._2) 
  
   
   val Arrayrdd= List(Array("00111","00111651","4444","PY","MA"),
                   Array("00111","00111651","4444","XX","MA"),
                   Array("00112","00112P11","5555","TA","MA"))
  val grouped1 = Arrayrdd.groupBy{_.take(3)} 
   val indexed1 = grouped1.mapValues(
      _.zipWithIndex
       .map {case (Array(a,b,c, rest@_*), idx) => Array(a,b,c+ (idx+1).toString) ++ rest})

    val unwrapped1 = indexed1.flatMap(_._2)
  }
}