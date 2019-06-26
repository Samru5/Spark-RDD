package Transformations_On_RDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MapPartitions_Example1 extends App{
  val conf = new SparkConf().setAppName("MapPartitions Example1").setMaster("local")
  val sc = new SparkContext(conf)

  val data=sc.parallelize(1 to 10,3)
  data.mapPartitions(x =>List(x.next).iterator).foreach(println)
}
