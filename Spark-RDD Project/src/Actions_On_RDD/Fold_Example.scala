package Actions_On_RDD

import org.apache.spark.{SparkContext, SparkConf}


object Fold_Example extends App{
  val conf = new SparkConf().setAppName("Fold Example").setMaster("local")
  val sc = new SparkContext(conf)

  val data = sc.parallelize(List(("maths", 80),("science", 90)))
  val additionalMarks = ("extra", 4)
  val sum = data.fold(additionalMarks){ (acc, marks) => val add = acc._2 + marks._2
    ("total", add)
  }
  println(sum)
}
