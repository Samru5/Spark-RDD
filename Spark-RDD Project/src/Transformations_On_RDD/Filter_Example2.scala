package Transformations_On_RDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object Filter_Example2 extends App{
  val conf = new SparkConf().setAppName("Filter Example2").setMaster("local")
  val sc = new SparkContext(conf)

  val myFile=sc.textFile("C:/Users/sashetye/demo/Errors.txt")
  myFile.flatMap(line => line.split(" ")).filter(value => value=="server").foreach(println)
}
