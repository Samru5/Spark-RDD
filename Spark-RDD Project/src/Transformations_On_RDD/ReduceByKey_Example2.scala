package Transformations_On_RDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object ReduceByKey_Example2 extends App{
  val conf = new SparkConf().setAppName("ReduceByKey_Example2").setMaster("local")
  val sc = new SparkContext(conf)

  //val myFile=sc.textFile("C:/Users/sashetye/demo/Employee.csv")
  val myFile = sc.textFile("C:/Users/sashetye/demo/Level1_Level2_Level3.csv")

  val rows=myFile.map(line => line.split(","))
  val data=rows.map(n =>(n(1),n(2))).reduceByKey((v1,v2) =>v1+v2)
  data.foreach(println)
}
