package project.imooc.ylqdh.bigdata.batch

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.sql.SparkSession

/**
  * 使用spark对HBase中的数据进行统计分析
  *   用了rdd的方式和DataFrame的方式
  *
  * 1.每个省份的访问量
  * 2.统计不同浏览器的访问量
  */
object AnalysisApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                  .appName("AnalysisApp").master("local[*]").getOrCreate()

    val day = "20190130"
    val tableName = "access_" + day

    val conf = new Configuration()
    conf.set("hbase.rootdir","hdfs://szgwnet01:9000/hbase")
    conf.set("hbase.zookeeper.quorum","szgwnet01:2181,szgwnet02:2181,szgwnet03:2181")
    conf.set(TableInputFormat.INPUT_TABLE,tableName)

    val scan = new Scan()

    // 设置要查询的cf
    scan.addFamily(Bytes.toBytes("y"))
    // 设置要查询的列
    scan.addColumn(Bytes.toBytes("y"),Bytes.toBytes("province"))
    scan.addColumn(Bytes.toBytes("y"),Bytes.toBytes("browserName"))

    // 设置Scan
    conf.set(TableInputFormat.SCAN,Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

    // 通过Spark的newAPIHadoopRDD方法读取数据
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

//    hbaseRDD.collect().foreach(x => {
//      val rowkey = Bytes.toString(x._1.get())
//      for(cell <- x._2.rawCells()){
//        val cf = Bytes.toString(CellUtil.cloneFamily(cell))
//        val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
//        val value = Bytes.toString(CellUtil.cloneValue(cell))
//
//        println(s"$rowkey : $cf : $qualifier : $value")
//      }
//    })

    /**
      * Spark 优化经常用到的一个点： Cache
      * 下面有两个地方用到的同一个rdd，可以先把这个rdd保存到下来
      */
    hbaseRDD.cache()

    // TODO... 1.每个省份的访问量   ==> 统计top10
    // 使用RDD的方式
    hbaseRDD.map(x=>{
      val province = Bytes.toString(x._2.getValue("y".getBytes,"province".getBytes))
      (province,1)
    }).reduceByKey(_+_)
        .map(x=>(x._2,x._1)).sortByKey(false)
        .map(x=>(x._2,x._1))
        .take(2).foreach(println)

    println("----------------------")


    // TODO...  2.统计浏览器的数量 top10
    hbaseRDD.map(x=>{
      val browserName = Bytes.toString(x._2.getValue("y".getBytes,"browserName".getBytes))
      (browserName,1)
    }).reduceByKey(_+_)
      .map(x=>(x._2,x._1)).sortByKey(false)
      .map(x=>(x._2,x._1))
      .collect().foreach(println)

    println("--------------spark sql---------------------")
    /**
      * 用Spark sql 方式统计以上两个需求
      */
    import spark.implicits._
    // TODO 需求1.
    hbaseRDD.map(x=>{
      val province = Bytes.toString(x._2.getValue("y".getBytes,"province".getBytes))
      ProvinceName(province)
    }).toDF().select("province").groupBy("province")
      .count().show()

    println("-------------------------------")

    // TODO 需求2.
    hbaseRDD.map(x=>{
      val browserName = Bytes.toString(x._2.getValue("y".getBytes,"browserName".getBytes))
      BrowserName(browserName)
    }).toDF.select("browserName")
      .groupBy("browserName")
      .count().show(false)

    // 还可以直接写sql
    println("``````````````````````````````````````````")
    hbaseRDD.map(x=>{
      val browserName = Bytes.toString(x._2.getValue("y".getBytes,"browserName".getBytes))
      BrowserName(browserName)
    }).toDF.createOrReplaceTempView("browserTable")
    spark.sql("select browserName,count(1) broCount from browserTable group by browserName order by broCount desc").show(false)


    //手动关闭rdd的cache
    hbaseRDD.unpersist()
    spark.stop()
  }

  case class ProvinceName(province:String)
  case class BrowserName(browserName:String)
}
