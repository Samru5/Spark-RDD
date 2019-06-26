package Actions_On_RDD

import org.apache.spark.{SparkContext, SparkConf}

object Collect_Example1 extends App{
  val conf = new SparkConf().setAppName("Collect Example1").setMaster("local")
  val sc = new SparkContext(conf)

  val list=sc.parallelize(List(1,29,3))
    list.flatMap(x => List(x,x,x)).collect().foreach(println)
}
