package Transformations_On_RDD

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Sample_Example extends App {
  val conf = new SparkConf().setAppName("Sample Example").setMaster("local")
  val sc = new SparkContext(conf)

  val data = sc.parallelize(1 to 10)
  data.sample(true, .2).count()
  data.foreach(println)

  data.sample(true,.1).foreach(println)
}
