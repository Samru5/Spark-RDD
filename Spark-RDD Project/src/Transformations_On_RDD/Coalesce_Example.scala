package Transformations_On_RDD

import org.apache.spark.{SparkContext, SparkConf}


object Coalesce_Example extends App{
  val conf = new SparkConf().setAppName("Coalesce Example").setMaster("local")
  val sc = new SparkContext(conf)

  val rdd1 = sc.parallelize(Array("jan","feb","mar","april","may","jun"),3)
  val result = rdd1.coalesce(4)
  result.foreach(println)
}
