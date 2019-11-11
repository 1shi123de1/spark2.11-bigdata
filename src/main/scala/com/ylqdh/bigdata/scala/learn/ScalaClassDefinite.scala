package com.ylqdh.bigdata.scala.learn

object ScalaClassDefinite {
  def main(args: Array[String]): Unit = {
    val waw = new student("wangwu",18,"math")
    println(waw.name + "\t" + waw.major)
    println(waw)
  }
}

// 在方法名后面的是主构造器
class people(val name:String,val age:Int) {
  println("perple constructure in")

  var gender = "male"
  val school:String = "ustc"

  // 附属构造器
  def this(name:String,age:Int,gender:String){
    // 附属构造器第一行要调用主构造器或者其他附属构造器
    this(name,age)
    this.gender = gender
  }
  println("perple constructure out")
}

// 继承
class student(name:String,age:Int,var major:String) extends people(name,age) {
  println("perple student constructure in")

  override val school:String = "ucl"

  // 重写父类的方法
  override def toString: String = "student tostring out...." + school

  println("perple student constructure out")
}
