package Actions_On_RDD

import org.apache.spark.{SparkContext, SparkConf}


object First_Example extends App{
  val conf = new SparkConf().setAppName("First Example").setMaster("local")
  val sc = new SparkContext(conf)

  val names=sc.parallelize(List("John","Alex","Seema","Alexander"))
  println(names.first())

  val myFile = sc.textFile("C:/Users/sashetye/demo/Level1_Level2_Level3.csv")
 println( myFile.map(line=>line.split(",")).map(n =>(n(0),n(3))).first())

}

