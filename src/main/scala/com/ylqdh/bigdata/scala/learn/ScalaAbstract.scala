package com.ylqdh.bigdata.scala.learn

object ScalaAbstract {
  def main(args: Array[String]): Unit = {
    val student2 = new student2()
    println(student2.age+student2.name+"\t"+student2.speak)
  }

}

/*
类的一个或者多个方法没有完整的实现(只有定义，没有实现)
 */
abstract class person {
  var name:String
  var age:Int
  def speak
}

class student2 extends person{
  override var name: String = "yyy"
  override var age: Int = 100

  override def speak: Unit = {
    println("speak implements")
  }
}