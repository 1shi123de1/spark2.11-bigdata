package com.ylqdh.bigdata.scala.learn

object applyApp {
  def main(args: Array[String]): Unit = {
//    for (i <- 1 to 10) {
//      applyTest.incr
//    }
//    println(applyTest.count)

    val a = applyTest()  // object -> apply

    val b = new applyTest()
    println(b)
    b()

    // 类()  --> 调的是object的apply
    // 对象() --> 调的是class的apply
  }
}

object applyTest {
  println("object applyTest in...")
  var count = 0
  def incr = {
    count = count +1
  }

  def apply() = {
    println("object applyTest apply method...")
  }


  println("object applyTest out...")
}

class applyTest {
  def apply() = {
    println("class applyTest apply method...")
  }
}