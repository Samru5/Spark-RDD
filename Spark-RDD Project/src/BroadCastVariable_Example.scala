import org.apache.spark.{SparkContext, SparkConf}

object BroadCastVariable_Example extends App {

  val conf = new SparkConf().setAppName("BroadCast variable").setMaster("local")
  val sc = new SparkContext(conf)

  val data = sc.broadcast(Array(4, 3, 1, 56))
  println(data.value.deep)
}
