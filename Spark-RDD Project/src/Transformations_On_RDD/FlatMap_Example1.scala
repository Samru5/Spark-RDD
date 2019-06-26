package Transformations_On_RDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object FlatMap_Example1 extends App{
  val conf = new SparkConf().setAppName("FlatMap Example1").setMaster("local")

  val sc = new SparkContext(conf)

  val list=sc.parallelize(List(4,1,9))
  list.flatMap(x => List(x,x,x)).foreach(println)
}
