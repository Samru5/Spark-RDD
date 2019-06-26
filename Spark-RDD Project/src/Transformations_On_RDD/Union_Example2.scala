package Transformations_On_RDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Union_Example2 extends App{
  val conf = new SparkConf().setAppName("Union Example2").setMaster("local")
  val sc = new SparkContext(conf)

  val l1=sc.parallelize(Seq((1,"Jan",2019),(23,"Dec",2013),(25,"Jul",2019)))
  val l2=sc.parallelize(Seq((5,"Sept",2016),(14,"Mar",2012)))
  l1.union(l2).foreach(println)





}
