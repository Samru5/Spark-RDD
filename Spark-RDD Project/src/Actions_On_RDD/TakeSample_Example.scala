package Actions_On_RDD

import org.apache.spark.{SparkContext, SparkConf}


object TakeSample_Example extends App{
  val conf = new SparkConf().setAppName("TakeSample Example").setMaster("local")
  val sc = new SparkContext(conf)

  val names=sc.parallelize(List("John","Alex","Seema","Alexander","Meena"))
  println(names.takeSample(true,3).deep)
  println(names.takeSample(false,3).deep)

  val myFile = sc.textFile("C:/Users/sashetye/demo/Level1_Level2_Level3.csv")
  myFile.map(line=>line.split(",")).map(n =>(n(0),n(3))).takeSample(true,14).foreach(println)

  myFile.map(line=>line.split(",")).map(n =>(n(0),n(3))).takeSample(false,25).foreach(println)

}

