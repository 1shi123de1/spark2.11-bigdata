package com.ylqdh.bigdata.scala.learn

object ScalaArrayTest extends App {
//  val a = new Array[String](3)
//  a(1) = "hello"
//  a.foreach(println)

//  val c = scala.collection.mutable.ArrayBuffer[Int]()
//
//  c += 1
//  c += 2
//  c += (3,4,5)
//  c ++= Array(6,7,8)
//
//  c.insert(1,0)
//  c.remove(7)
//  c.remove(0,3)
//  c.trimEnd(2)

//  for (i <- 0 until c.length) {
//    println(c(i))
//  }
//  for (ele <- c) {
//    println(ele)
//  }
//
//  for (i <- (0 until c.length).reverse) {
//    println(c(i))
//  }

  val l = scala.collection.mutable.ListBuffer[Int]()
  l += 2
  l += (3,4,5)
  l ++= List(6,7,8,9)

  l -= 2
  l -= 3

  l.isEmpty
  l.head
  l.tail
  l.tail.head

  val s = scala.collection.mutable.Set[Int]()
  s += 1
  s += 2
  s ++= Set(3,4,2)    // 重复的元素不会被加入到set中
  s.head
  s.tail

  for (ele <- s) {
    println(ele)
  }


}
