package Actions_On_RDD

import org.apache.spark.{SparkContext, SparkConf}


object Reduce_Example extends App{
  val conf = new SparkConf().setAppName("Reduce").setMaster("local")
  val sc = new SparkContext(conf)

  val names=sc.parallelize(List("John","Alex","Seema","Alexander","Ajay"))
  names.reduce((t1,t2) =>t1+t2).foreach(print)

  //or
  names.reduce(_+_).foreach(print)


  val list=sc.parallelize(List(20,10,40))
  println(list.reduce(_+_))


}

