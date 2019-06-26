package Actions_On_RDD

import org.apache.spark.{SparkContext, SparkConf}


object CountByKey_Example1 extends App{
  val conf = new SparkConf().setAppName("CountByKey Example").setMaster("local")
  val sc = new SparkContext(conf)

  val names=sc.parallelize(List("John","Alex","Seema","Alexander","Meena","Alex","Benny"))
names.map(words =>(words,1)).countByKey().foreach(println)
}

