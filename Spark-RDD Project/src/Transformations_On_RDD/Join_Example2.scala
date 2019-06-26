package Transformations_On_RDD

import org.apache.spark.{SparkContext, SparkConf}


object Join_Example2 extends App{
  val conf = new SparkConf().setAppName("Join Example1").setMaster("local")
  val sc = new SparkContext(conf)

  val names=sc.parallelize(List("abe","abby","apple")).map(a =>(a,1))

  val names2=sc.parallelize(List("apple","beaty","beatrice")).map(a =>(a,1))

  names.join(names2).collect().foreach(println)

  names.leftOuterJoin(names2).collect().foreach(println)

  names.rightOuterJoin(names2).collect().foreach(println)

}
