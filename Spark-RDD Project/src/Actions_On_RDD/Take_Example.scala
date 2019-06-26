package Actions_On_RDD

import org.apache.spark.{SparkContext, SparkConf}


object Take_Example extends App{
  val conf = new SparkConf().setAppName("Take Example").setMaster("local")
  val sc = new SparkContext(conf)

  val names=sc.parallelize(List("John","Alex","Seema","Alexander"))
  println(names.take(2).deep)

  val myFile = sc.textFile("C:/Users/sashetye/demo/Level1_Level2_Level3.csv")
  myFile.map(line=>line.split(",")).map(n =>(n(0),n(3))).take(5).foreach(println)

}

