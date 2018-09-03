package spark.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions.explode

object xml
{
  
  def main(args:Array[String])
  {
       //xml data read,parse,save as csv file
       val conf = new SparkConf().setAppName("xmloperations").setMaster("local")
       val sc = new SparkContext(conf)
       sc.setLogLevel("ERROR")
       val sqlContext = new SQLContext(sc)
       import sqlContext.implicits._
       val df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "Transaction").load("file:/home/hduser/sparkdata/student.xml")
       df.printSchema()
       //df.show()
       val flattened = df.withColumn("LineItem", explode($"stud.LineItem"))
       val selectedData = flattened.select($"name.LineItem",$"dept.LineItem",$"rno.LineItem")
       selectedData.show(1,false)
       //selectedData.write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save("file:/home/hduser/sparkdata/transactions.csv")
  }
}