package Transformations_On_RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object ReduceByKey_Example1 extends App{
  val conf = new SparkConf().setAppName("ReduceByKey Example1").setMaster("local")
  val sc = new SparkContext(conf)

  val list=sc.parallelize(List("One","Five","Two","Eleven","One","Nine","Five"))
  list.map(k =>(k,1)).reduceByKey(_ + _).foreach(println)
}
