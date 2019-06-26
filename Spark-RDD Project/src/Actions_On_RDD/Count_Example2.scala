package Actions_On_RDD
import org.apache.spark.{SparkContext, SparkConf}


object Count_Example2 extends App{
  val conf = new SparkConf().setAppName("Count Example2").setMaster("local")
  val sc = new SparkContext(conf)

  //val myFile=sc.textFile("C:/Users/sashetye/demo/Errors.txt")
  val myFile = sc.textFile("C:/Users/sashetye/demo/Level1_Level2_Level3.csv")

  //myFile.flatMap(line =>line.split(" ")).map(value=>value=="server")
  myFile.flatMap(line =>line.split(" ")).map(value=>value=="0.007729")

  println( myFile.count())
}
