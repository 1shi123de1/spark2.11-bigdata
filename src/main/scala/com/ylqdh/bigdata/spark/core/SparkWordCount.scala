package com.ylqdh.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-word-count").setMaster("local[1]")

    val sc = new SparkContext(conf)
    //    sc.textFile(args[0])
//    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_,1).sortBy(_._2,false).saveAsTextFile(args(1))
    val textRDD = sc.textFile(args(0),1)
    val wordRDD = textRDD.flatMap(_.split(" "))
    val pairRDD = wordRDD.map((_,1))
    val resultRDD = pairRDD.reduceByKey(_+_,1).sortBy(_._2,false)
    resultRDD.saveAsTextFile(args(1))
    sc.stop()
  }
}
