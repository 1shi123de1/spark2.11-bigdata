package com.ylqdh.bigdata.spark.streaming

object ScalaSlidingTest {
  def main(args: Array[String]): Unit = {
    val ints = List(1,2,3,4,5,6)

    // 滑动窗口函数,第一个参数窗口大小，第二个参数是步长
    var intses: Iterator[List[Int]] = ints.sliding(2, 2)

    for (elem <- intses) {
      println(elem.mkString(","))
    }
  }
}
