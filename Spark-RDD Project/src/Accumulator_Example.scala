import org.apache.spark.{SparkContext, SparkConf}

object Accumulator_Example extends App {
  val conf = new SparkConf().setAppName("My Accumulator").setMaster("local")
  val sc = new SparkContext(conf)

  val accum = sc.longAccumulator("My Accumulator")
  sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x))
  println(accum.value)
}
