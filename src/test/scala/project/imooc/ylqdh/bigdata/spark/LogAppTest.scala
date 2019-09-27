package project.imooc.ylqdh.bigdata.spark

import project.imooc.ylqdh.bigdata.batch.LogApp

object LogAppTest {
  def main(args: Array[String]): Unit = {
    val ip = "110.85.18.234"
    val referer = "https://www.imooc.com/course/list?c=op|Sogou"
    val url = "/activity/newcomer"
    val bytesent = "2354"

    val rowkey = LogApp.getRowKey("20190130",referer+url+ip+bytesent)
    println(rowkey)

  }

}
