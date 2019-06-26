package Transformations_On_RDD


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MapPartitions_Example2 extends App {
  val conf = new SparkConf().setAppName("MapPartitions Example2").setMaster("local")
  val sc = new SparkContext(conf)

  val data = sc.parallelize(1 to 10)
  data.mapPartitions(x => List(x.next).iterator).foreach(println)
}



