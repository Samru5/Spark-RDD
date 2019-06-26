package Transformations_On_RDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Filter_Example1 extends App{
  val conf = new SparkConf().setAppName("Filter Example1").setMaster("local")
  val sc = new SparkContext(conf)

  //val myFile=sc.textFile("C:/Users/sashetye/demo/Errors.txt")
  val myFile = sc.textFile("C:/Users/sashetye/demo/Level1_Level2_Level3.csv")

  // myFile.filter(line =>line.contains("ERROR")).foreach(println)
  myFile.filter(line =>line.contains("48.62044629")).foreach(println)

}
