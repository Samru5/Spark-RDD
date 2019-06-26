package Actions_On_RDD
import org.apache.spark.{SparkContext, SparkConf}


object Count_Example1 extends App{
  val conf = new SparkConf().setAppName("Count Example1").setMaster("local")
  val sc = new SparkContext(conf)

val names=sc.parallelize(List("Benny","Alex","John","Meena"))
  println(names.count())
}
