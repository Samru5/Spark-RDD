package Transformations_On_RDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object GroupByKey_Example1 extends App{
  val conf = new SparkConf().setAppName("GroupByKey Example1").setMaster("local")
  val sc = new SparkContext(conf)

  val data=sc.parallelize(Array(('k',3),('A',2),('T',4),('k',5),('T',9)))
  data.groupByKey().foreach(println)
}
