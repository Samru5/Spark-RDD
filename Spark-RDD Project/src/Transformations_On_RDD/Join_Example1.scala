package Transformations_On_RDD

import org.apache.spark.{SparkContext, SparkConf}


object Join_Example1 extends App{
  val conf = new SparkConf().setAppName("Join Example1").setMaster("local")
  val sc = new SparkContext(conf)

  val data = sc.parallelize(Array(('A',1),('b',2),('c',3)))
  val data2 =sc.parallelize(Array(('A',4),('A',6),('b',7),('c',3),('c',8)))
  val result = data.join(data2)
  println(result.collect().mkString(","))

}
