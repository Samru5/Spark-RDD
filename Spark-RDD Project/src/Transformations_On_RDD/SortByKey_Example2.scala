package Transformations_On_RDD

import org.apache.spark.{SparkContext, SparkConf}

object SortByKey_Example2 extends App{
  val conf = new SparkConf().setAppName("SortByKey_Example2").setMaster("local")
  val sc = new SparkContext(conf)

  val data=sc.parallelize(Seq(("maths",52), ("english",75), ("science",82), ("computer",65), ("maths",85)))

data.sortByKey().foreach(println)
}
