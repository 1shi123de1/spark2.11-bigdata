package com.ylqdh.bigdata.scala.learn

object ScalaExecptionApp extends App {
  try {
    val i = 10 / 0
    println(i)
  }catch {
    case e:ArithmeticException => println("除数不能为0")
    case e:Exception => println(e.printStackTrace())
  }finally {
    //释放资源，一定能执行
  }


}
