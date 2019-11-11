package com.ylqdh.bigdata.scala.learn

object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("Hello World...")

//    println(speed(12))
//
//    println(sum(2,3))
//    println(sum(2,3,4))
//    println(sum(2))

//    for (i <- 1 until 10 if i%2==0) {
//      println(i)
//    }

//    val courses = Array("hadoop","spark","java","scala")
//    for (course <- courses) {
//      println(course)
//    }
//    courses.foreach(x => println(x))

    var num = 100
    var sum = 0
    while (num > 0) {
      sum += num
      num -= 1
    }
    println(sum)
  }

  def speed(instance:Double ,time:Double = 120):Double = {
    instance / time
  }

  def sum(in:Int*) = {
    var result = 0
    for (num <- in) {
      result += num
    }
    result
  }
}

