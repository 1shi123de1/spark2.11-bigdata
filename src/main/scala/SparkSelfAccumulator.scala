import java.util

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object SparkSelfAccumulator {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("selfAccumulator")
    val sc = new SparkContext(config)

    val dataRDD = sc.makeRDD(Array(1,2,3,4,5,6),3)
    val strRDD = sc.makeRDD(Array("ylqdh hbase","hadoop","ylq learn","spark"),2)

    // 自带long类型累加器计算求和
    var accumulator = sc.longAccumulator
    dataRDD.foreach{
      case i=>{
        accumulator.add(i)
      }
    }
    println("sum = "+accumulator.value)

    // 自定义的累加器过滤需要的字符
    val wordAccumulator = new WordAccumulator
    // 注册累加器
    sc.register(wordAccumulator)
    strRDD.foreach{
      case word => {
        //执行累加器的累加功能
        wordAccumulator.add(word)
      }
    }
    println("after filter,string = "+wordAccumulator.value)

    sc.stop()
  }
}

// 继承 AccumulatorV2
// 实现抽象方法
class WordAccumulator extends AccumulatorV2[String,util.ArrayList[String]]{
  val list = new util.ArrayList[String]()

  // 当前累加器是否是初始化状态
  override def isZero: Boolean = {
    list.isEmpty
  }

  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator()
  }

  // 重置累加器
  override def reset(): Unit = {
    list.clear()
  }

  // 向累加器中增加数据
  override def add(v: String): Unit = {
    if (v.contains("ylq")){
      list.add(v)
    }
  }

  // 合并累加器
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  // 获取累加器的结果
  override def value: util.ArrayList[String] = list
}
