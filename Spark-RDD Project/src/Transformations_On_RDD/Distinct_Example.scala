package Transformations_On_RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Distinct_Example1 extends App{
  val conf = new SparkConf().setAppName("Distinct Example1").setMaster("local")
  val sc = new SparkContext(conf)

  val l1=sc.parallelize(1 to 9)
  val l2=sc.parallelize(5 to 15)

l1.union(l2).distinct().foreach(println)
}
