package Transformations_On_RDD


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object FlatMap_Example2 extends App {
  val conf = new SparkConf().setAppName("FlatMap Example2").setMaster("local")
  val sc = new SparkContext(conf)

  //val myFile = sc.textFile("C:/Users/sashetye/demo/Employee.csv")
  val myFile = sc.textFile("C:/Users/sashetye/demo/Level1_Level2_Level3.csv")

  myFile.flatMap(line => line.split(",")).foreach(println)

}
