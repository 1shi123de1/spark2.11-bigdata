package com.ylqdh.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object SparkActionOperators {
  def main(args: Array[String]): Unit = {
    val config:SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkActionOperator")
    val sc = new SparkContext(config)

    // 1. reduce
    // 通过函数聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据
    val reRDD = sc.makeRDD(1 to 8,2)
//    val reduceRDD = reRDD.reduce(_+_)
    reRDD.reduce(_+_)

    // 2. collect
    // 在驱动程序中，以数组形式返回RDD所有元素

    // 3. count
    // 返回RDD中元素的个数

    // 4. first
    // 返回RDD中的第一个元素

    // 5. take(n)
    // 返回一个由RDD的前n个元素组成的数组

    // 6. takeOrdered(n)
    // 返回RDD排序后的前n个元素组成的数组
    // 和上一个对比，take不排序取，这个是排序后取数据

    // 7. aggregate
    // 三个参数，初始值，seqOp,combOp
    // 将每个分区里面的元素通过seqOp和初始值进行聚合，然后用combine函数将每个分区的结果和初始值进行combine操作
    // 初始值在分区内会加上，分区间的计算也会加上
//    val rdd1 = sc.makeRDD(1 to 10,2)
//    val ardd1 = rdd1.aggregate(0)(_+_,_+_)  // 初始值为0 的所有元素相加
//    val ardd2 = rdd1.aggregate(10)(_+_,_+_) // 初始值为10的所有元素相加

    // 8. fold
    // 和上面的结果一样，参数变为两个，分区内和分区间共用同一个函数
    val rdd1 = sc.makeRDD(1 to 10,2)
    val frdd1 = rdd1.fold(0)(_+_)
    val frdd2 = rdd1.fold(10)(_+_)

    // 9. saveAsTextFile(Path)
    // 把结果保存成文本文件

    // 10. saveAsSequenceFile(Path)
    // 把结果保存成序列化文件

    // 11. saveAsObjectFile(Path)
    // 把结果保存成对象文件

    // 12. countByKey
    // PairRDD,统计同一个key相同的个数
    val crdd = sc.parallelize(List(("a",2),("a",1),("a",1),("b",1),("b",1),("c",1)))
    val countbykey = crdd.countByKey()

    // 13. foreach
    // 在数据集的每一个元素上，运行函数进行更新
  }
}
