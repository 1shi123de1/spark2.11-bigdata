package project.weblog.ylqdh.bigdata.sparkstreamingTest

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
    过滤掉指定名字的黑名单功能
    数据结构是,{时间，名字} => {20191111,xiaoming}
    如果黑名单上有yyl，那么这条数据应该被过滤掉

    具体做法如下：
    1、黑名单名字转换为pairRDD : xiaoming =>(xiaoming,true)
    2、源数据转换: (20191111,xiaoming) => (xiaoming,<20191111,xiaoming>)
    3、两个数据使用transform进行join操作： {xiaoming,(<20191111,xiaoming>,true)}
    4、判断标志位，为true的是黑名单，不为true的不是黑名单，正常输出
 */
object BlackFilter {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("BlackFilter").setMaster("local[*]")

    /*
     创建StreamingContext需要两个参数：SparkConf、batch interval
    */
    val ssc = new StreamingContext(sparkconf,Seconds(5))

    // 黑名单的名字，生产上一般是写在数据库中的，从数据库中读取
    val black = List("qqq","hhh")
    val blackRDD = ssc.sparkContext.parallelize(black).map(x => (x,true))

    // 数据来源
    val lines = ssc.socketTextStream("172.16.13.152",8989)

    //
    val maplines = lines.map(x => (x.split(",")(1),x))    // 构建成{xiaoming,(20191111,xiaoming)}的数据结构
                        .transform(rdd => rdd.leftOuterJoin(blackRDD)    // 左连接
                                             .filter(x => x._2._2.getOrElse(false) != true)   // 过滤掉标志位是true的数据
                                              .map(x => x._2._1)         // 取出{xiaoming,(<20191111,xiaoming>,true)}中原本的数据
                                    )

    maplines.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
