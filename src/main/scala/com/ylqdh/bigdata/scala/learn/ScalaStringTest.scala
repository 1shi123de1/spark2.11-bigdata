package com.ylqdh.bigdata.scala.learn

object ScalaStringTest extends App {
  // 字符串换行
  val b =
    """
      |this is a
      |mutil line
      |string
    """.stripMargin
  println(b)

  val a = 123
  println(s"price:$a")

}
