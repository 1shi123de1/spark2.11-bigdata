package project.weblog.ylqdh.bigdata.streaming

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/*
  日期格式化：把yyyy-MM-dd HH:mm:ss 格式化成 yyyyMMddHHmmss
 */
object DateUtil {

  val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")

  def getTime(time:String) = {
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }
  def parseToMinute(time:String) =  {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {
    println(parseToMinute("2019-11-25 11:07:09"))
  }

}
