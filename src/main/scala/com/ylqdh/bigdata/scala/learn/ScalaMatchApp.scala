package com.ylqdh.bigdata.scala.learn

object ScalaMatchApp extends App {
//  val name = "ylq"
//  name match {
//    case "ylq" => println("It's me.")
//    case "yyy" => println("Who's you?")
//    case "qqq" => println("Oh,I know you.")
//    case _	=> println("????")
//  }
//
//  def judgeGrade(grade:String): Unit ={
//    grade match {
//      case "A" => println("Execllent")
//      case "B" => println("Good")
//      case "C" => println("Just so so")
//      case "D" => println("Pass")
//      case _ => println("Fail")
//    }
//  }
//
//  judgeGrade("A")
//  judgeGrade("D")
//  judgeGrade("E")

//  def greeting(array:Array[String]): Unit ={
//    array match {
//      case Array("ylq") => println("Hi,ylq")
//      case Array(x,y) => println("Hi,"+x+" and "+y)
//      case Array("ylq",_*) => println("Hi,ylq and other friends")
//      case _ => println("Hi,everybody")
//    }
//  }
//  greeting(Array("ylq"))
//  greeting(Array("ylq","yyy"))
//
//  def matchType(obj:Any): Unit ={
//    obj match {
//      case x:Int => println("Int")
//      case x:String => println("String")
//      case m:Map[_,_] => m.foreach(println)
//      case _ => println("other type")
//    }
//  }
//  matchType(1)
//  matchType("hi")
//  matchType(2.3)
//  matchType(Map("name"->"ylq"))


  class Person
  case class CTO(name:String,floor:Int) extends Person
  case class Employee(name:String,floor:Int) extends Person
  case class Other(name:String,floor:Int) extends Person

  def caseClassMatch(person:Person): Unit ={
    person match {
      case CTO(name,floor) => println(name+" is cto,floor is "+floor)
      case Employee(name,floor) => println(name+" is employee,floor is "+floor)
      case Other(name,floor) => println(name+" is other employee")
    }
  }

  caseClassMatch(CTO("ylq",45))
  caseClassMatch(Employee("zhangsna",12))
  caseClassMatch(Other("baoan",1))

  val names = Map("ylq"->100,"yyy"->90,"qqq"->85)
  def grades(name:String): Unit ={
    var grade = names.get(name)
    grade match {
      case Some(grade) => println(name+",your grade is "+grade)
      case None => println("sorry,your grade is not in system.")
    }
  }
  grades("ylq")
  grades("qy")
}
