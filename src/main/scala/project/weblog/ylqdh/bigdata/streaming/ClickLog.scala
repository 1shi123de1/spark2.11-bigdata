package project.weblog.ylqdh.bigdata.streaming

/**
  * 清洗后的日志信息格式
  * @param ip   日志的访问IP地址
  * @param time 日志访问的时间
  * @param courseID   日志访问的课程号
  * @param statusCode 日志访问的状态
  * @param referer    日志访问的referer
  */

case class ClickLog (ip:String,time:String,courseID:Int,statusCode:Int,referer:String)
