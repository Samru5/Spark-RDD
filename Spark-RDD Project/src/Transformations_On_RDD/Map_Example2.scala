package Transformations_On_RDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Map_Example2 extends App{
  val conf = new SparkConf().setAppName("Map Example2").setMaster("local")

  val sc = new SparkContext(conf)

 // val myFile = sc.textFile("C:/Users/sashetye/demo/Employee.csv")
  val myFile = sc.textFile("C:/Users/sashetye/demo/Level1_Level2_Level3.csv")

  myFile.map(line => line.length).foreach(println)


}
