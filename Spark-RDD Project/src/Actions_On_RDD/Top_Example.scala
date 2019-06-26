package Actions_On_RDD

import org.apache.spark.{SparkContext, SparkConf}


object Top_Example extends App{
  val conf = new SparkConf().setAppName("Top Example").setMaster("local")
  val sc = new SparkContext(conf)

  val names=sc.parallelize(List("John","Alex","Seema","Alexander"))
  println(names.top(2).deep)

  val myFile = sc.textFile("C:/Users/sashetye/demo/Level1_Level2_Level3.csv")
  myFile.map(line=>line.split(",")).map(n =>(n(0),n(3))).top(2).foreach(println)

}

