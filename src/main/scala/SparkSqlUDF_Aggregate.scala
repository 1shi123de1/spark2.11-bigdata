import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

object SparkSqlUDF_Aggregate {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSqlUDF_Aggregate")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 自定义聚合函数
    // 创建聚合函数对象
    val udfa = new MyAggregateFunction
    // 注册聚合函数
    sparkSession.udf.register("avgAge",udfa)
    // 使用聚合函数
    val frame = sparkSession.read.json("in/weather.json")
    frame.createOrReplaceTempView("weather")
    sparkSession.sql("select avgAge(number) from weather").show()

    sparkSession.stop()
  }
}

// 用户自定义聚合函数，计算用户平均年龄
// 1）继承UserDefinedAggregateFunction
// 2) 实现方法
class MyAggregateFunction extends UserDefinedAggregateFunction{

  // 函数输入的数据结构
  override def inputSchema: StructType = {
    new StructType().add("age",LongType)
  }

  // 计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType).add("count",LongType)
  }

  // 函数返回的数据类型
  override def dataType: DataType = DoubleType

  // 函数是否稳定
  override def deterministic: Boolean = true

  // 计算之前的缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 1L
  }

  // 根据查询结果更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  // 将多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // sum
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    // count
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
