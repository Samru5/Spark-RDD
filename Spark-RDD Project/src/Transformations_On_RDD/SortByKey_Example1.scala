package Transformations_On_RDD



import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object SortByKey_Example1 extends App{
  val conf = new SparkConf().setAppName("SortByKey_Example1").setMaster("local")
  val sc = new SparkContext(conf)

  //val myFile=sc.textFile("C:/Users/sashetye/demo/Employee.csv")
  val myFile = sc.textFile("C:/Users/sashetye/demo/Level1_Level2_Level3.csv")

  val rows=myFile.map(line => line.split(","))
  val data=rows.map(n =>(n(1),n(2))).sortByKey(true)
  data.foreach(println)

  val data2=rows.map(n =>(n(0),n(2))).sortByKey(false)
  data2.foreach(println)


}

