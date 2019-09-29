package project.imooc.ylqdh.bigdata.streaming.utils

import java.util
import java.util.{Date, UUID}

import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.time.FastDateFormat

import scala.util.Random

/*
    生成mock付费日志
 */
object MakeMockData {
  def main(args: Array[String]): Unit = {
    val random = new Random()
    val dateFormat = FastDateFormat.getInstance("yyyyMMddHHmmss")

    // time,userid,courseid,orderid,fee,flag
    for(i <- 0 to 9) {
      val time = dateFormat.format(new Date())
      val userid = random.nextInt(1000)
      val courseid = random.nextInt(500)
      val fee = random.nextInt(400)
      val orderid = UUID.randomUUID().toString
      val result = Array("0","1")
      val flag = result(random.nextInt(2))

      val map = new util.HashMap[String,Object]()
      map.put("time",time)
      map.put("userid",userid)
      map.put("courseid",courseid)
      map.put("fee",fee)
      map.put("orderid",orderid)
      map.put("flag",flag)

      val json = new JSONObject(map)
      println(json)
    }
  }
}
