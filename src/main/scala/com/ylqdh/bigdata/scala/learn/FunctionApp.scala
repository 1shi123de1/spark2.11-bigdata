package com.ylqdh.bigdata.scala.learn

object FunctionApp extends App {
  def sum(a:Int)(b:Int)(c:Int) = a+b+c

//  println(sum(1)(3)(4))


  val txt = scala.io.Source.fromFile("C:\\svn\\software\\test.txt").mkString
  val txts = List(txt)
//  println(txts)
  val pairs = txts.flatMap(_.split("\r\n")).flatMap(_.split(","))  // 文件中有多行，先按回车换行符把所有内容分隔成一行，再按逗号分隔成一个一个的单词
    .map(x => (x,1))                                 // 把单词形成键值对 (单词,1)
//    .groupBy(x => x._1)                           //   键聚合,groupBy(_._1) ,结果 (String,List(String,1))
//    .map(x => (x._1,x._2.size))                   //    计算值的size，就是个数
//        .mapValues(_.size)
//    .foreach(println)

 pairs.groupBy(_._1)
}
