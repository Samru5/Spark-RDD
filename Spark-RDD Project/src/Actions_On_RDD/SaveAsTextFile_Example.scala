package Actions_On_RDD

import org.apache.spark.{SparkContext, SparkConf}


object SaveAsTextFile_Example extends App{
  val conf = new SparkConf().setAppName("SaveAsTextFile_Example").setMaster("local")
  val sc = new SparkContext(conf)

  val myFile = sc.textFile("C:/Users/sashetye/demo/Colours.txt")
  myFile.flatMap(line =>line.split(" ")).saveAsTextFile("ResultData.txt")
}

