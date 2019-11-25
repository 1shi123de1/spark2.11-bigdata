package project.weblog.ylqdh.bigdata.sparkstreamingTest

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
  使用文件系统测试spark streaming的wordcount
 */
object FileWordCount {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("FileWordCount").setMaster("local[*]")

    /*
     创建StreamingContext需要两个参数：SparkConf、batch interval
    */
    val ssc = new StreamingContext(sparkconf,Seconds(5))

    /* 参数是一个目录
        会监控目录下的文件，目前只支持文件增加，文件内容增加的数据不支持读取，嵌套的文件夹也不支持
       注意如下：
        1、文件的数据格式要相同
        2、文件要原子性的添加或者拷贝进监控的目录
        3、一旦文件移动进目录，则不能被修改

        文件系统生成DStreams不需要receiver，因此可以使用local、local[1]
     */
    val lines = ssc.textFileStream("")

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
