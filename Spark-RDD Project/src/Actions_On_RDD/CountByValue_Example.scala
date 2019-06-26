package Actions_On_RDD

import org.apache.spark.{SparkContext, SparkConf}


object CountByValue_Example extends App{
  val conf = new SparkConf().setAppName("CountByKey Example2").setMaster("local")
  val sc = new SparkContext(conf)

  //val myFile = sc.textFile("C:/Users/sashetye/demo/Employee.csv")
  val myFile = sc.textFile("C:/Users/sashetye/demo/Level1_Level2_Level3.csv")

  myFile.map(line=>line.split(",")).map(n =>(n(1),n(3))).countByValue().foreach(println)
}

