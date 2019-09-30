package project.imooc.ylqdh.bigdata.streaming.spark

import java.util
import java.util.{Date, Properties, UUID}

import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import project.imooc.ylqdh.bigdata.streaming.utils.ParamsConf

import scala.util.Random

object KafkaProducerApp {
  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    prop.put("bootstrap.servers",ParamsConf.brokers)
    prop.put("request.required,acks","1")
    val topic = ParamsConf.topic
    val producer = new KafkaProducer[String,String](prop)

    val random = new Random()
    val dateFormat = FastDateFormat.getInstance("yyyyMMddHHmmss")
    for (i <- 1 to 100) {
      val time = dateFormat.format(new Date())
      val userid = random.nextInt(1000)+""
      val courseid = random.nextInt(500)+""
      val fee = random.nextInt(400)+""
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

      producer.send(new ProducerRecord[String,String](topic(0),1+"",json.toString))
    }
  }

}
