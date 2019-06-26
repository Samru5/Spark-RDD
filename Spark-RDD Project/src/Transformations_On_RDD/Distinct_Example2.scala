package Transformations_On_RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Distinct_Example2 extends App{
  val conf = new SparkConf().setAppName("Distict Example2").setMaster("local")
  val sc = new SparkContext(conf)


  val data = sc.parallelize(Seq((1,"jan",2016),(3,"nov",2014),(16,"feb",2014),(3,"nov",2014)))
  val result = data.distinct()
  println(result.collect().mkString(", "))
}
