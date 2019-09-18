import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, TypedColumn}

object SparkSqlUDF_classAggredate {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSqlUDF_classAggredate").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._
    // 创建聚合函数对象
    val udaf = new MyAgeAvgClassFunction

    // 将聚合函数转换为查询列
    val avgCol:TypedColumn[WeatherBean,Double] = udaf.toColumn.name("avgAge")

    val frame = sparkSession.read.json("in/weather.json")

    val weatherDS = frame.as[WeatherBean]

    weatherDS.select(avgCol).show()

    sparkSession.stop()
  }

}

case class WeatherBean (id:BigInt,city:String,weather:String,number:BigInt,spot:String)
case class AvgBuffer(var sum:BigInt,var count: Int)

// 声明用户自定义聚合函数(强类型)
// 继承 Aggregateor
// 实现方法
class MyAgeAvgClassFunction extends Aggregator[ WeatherBean,AvgBuffer,Double ]{

  // 初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0,0)
  }

  // 聚合数据
  override def reduce(b: AvgBuffer, a: WeatherBean): AvgBuffer = {
    b.sum = b.sum + a.number
    b.count = b.count + 1
    b
  }

  // 缓冲区的合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  // 完成计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
