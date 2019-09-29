package project.imooc.ylqdh.bigdata.batch

import java.util.zip.CRC32
import java.util.{Date, Locale}

import com.sun.javafx.util.Logging
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, KeyValue, TableName}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import project.imooc.ylqdh.bigdata.utils.LogFormat
import org.apache.hadoop.mapreduce.{Job => NewAPIHadoopJob}

import scala.collection.mutable.ListBuffer

/*
    对日志进行ETL操作：把数据从文件系统(本地、HDFS)拿到进行清洗(ip解析、ua解析、time格式化)之后，最终存储到HBase中

    批处理：一般一天处理一次，今天凌晨来处理昨天一天的数据
    需要传给LogApp一个处理时间：yyyyMMdd
    HBase表：一天一个表，表名格式为 logs_yyyyMMdd
    后续进行业务统计分析时，也是按天，一天一个批次，直接从HBase表(logs_yyyyMMdd)里读取数据，然后使用Spark进行业务统计即可

    HBase表需要的操作：
        创建表
        rowkey设计
            .结合项目的业务需求来
            .通常是组合使用：时间作为rowkey前缀，拼上字段的MD5/CRC32值
        列族名cf：这里使用y，设计时不要太多，通常一个或两个
        column：把文件系统上解析出来的df的字段放到Map中，然后循环拼成一个rowkey对应的cf下的所有列
 */

// 优化： 直接生成HFile
object LogETLAppV3 extends Logging {

  def main(args: Array[String]): Unit = {

//    if (args.length != 1) {
//      println("Usage: LogApp <time>")
//      System.exit(1)
//    }


//    val day = args(0)   // 先写死，后续通过shell 脚本传递给 spark-submit 参数获得
//    val input = s"hdfs://szgwnet01:9000/access/$day/*"
    val day = "20190130"
    val input = "in/access_format.log"

    val spark = SparkSession.builder().config("spark.serializer","org.apache.spark.serializer.KryoSerializer").appName("LogApp").master("local[*]").getOrCreate()
//    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    /*
    自己定义format里面的解析类，我知道怎么解析，但不知道封装为能spark调用的规范
    所以在Java中写方法处理文件，然后在spark类中调用方法
    spark直接读处理好的文件
    val logDF = spark.read.format("解析class").option("path","in/access.log").load()
     */

    //spark.read.format("").option("","").load()
    // 在Java中处理日志为指定格式
    val logfomat = new LogFormat()
    logfomat.format("in/access.log",input)

    // spark读取处理好的日志文件，并把txt格式的文件转换为DataFrame
    val txtRDD = spark.sparkContext.textFile(input)
    val schemaString = "ip,province,city,operator,time,url,method,protocal,status,bytesent,referer,browserName,browserVersion,osName,osVersion"
    val fields = schemaString.split(",").map(fieldName => StructField(fieldName,StringType,nullable = true))
    val schema = StructType(fields)
    val rowRDD = txtRDD.map(_.split("\t"))
                      .map(cells => Row(cells(0),cells(1),cells(2),cells(3),cells(4),cells(5),cells(6),cells(7),cells(8),cells(9),cells(10),cells(11),cells(12),cells(13),cells(14)))
    var logDF = spark.createDataFrame(rowRDD,schema)

    // udf函数 转换时间格式
    import org.apache.spark.sql.functions._
    def formatTime() = udf((time:String) => {
      FastDateFormat.getInstance("yyyyMMdd HH:mm:ss").format(new Date(FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
                    .parse(time)getTime))
    })

    // 在原来数据上添加或修改字段，把格式化后的时间加上
    // 如果加上的字段名字和原来的名字一样就是修改，不一样则在最后添加上新字段
    logDF = logDF.withColumn("formattime",formatTime()(logDF("time")))

//    logDF.createOrReplaceTempView("logView")
//    spark.sql("select ip,time,status from logView").show()

    // TODO 上面把日志数据清洗好了，把它存进HBase中
    // 涉及到HBase表的设计，需要多少个列族，哪些字段放进哪些列族，rowkey如何设计
    val hbaseInfoRDD = logDF.rdd.mapPartitions(partition => {

      partition.flatMap(x=>{
        val ip = x.getAs[String]("ip")
        val province = x.getAs[String]("province")
        val city = x.getAs[String]("city")
        val operator = x.getAs[String]("operator")
        val time = x.getAs[String]("time")
        val url = x.getAs[String]("url")
        val method = x.getAs[String]("method")
        val protocal = x.getAs[String]("protocal")
        val status = x.getAs[String]("status")
        val bytesent = x.getAs[String]("bytesent")
        val referer = x.getAs[String]("referer")
        val browserName = x.getAs[String]("browserName")
        val browserVersion = x.getAs[String]("browserVersion")
        val osName = x.getAs[String]("osName")
        val osVersion = x.getAs[String]("osVersion")
        val formattime = x.getAs[String]("formattime")

        val columns = scala.collection.mutable.HashMap[String,String]()
        columns.put("ip",ip)
        columns.put("province",province)
        columns.put("city",city)
        columns.put("operator",operator)
        columns.put("time",time)
        columns.put("url",url)
        columns.put("method",method)
        columns.put("protocal",protocal)
        columns.put("status",status)
        columns.put("bytesent",bytesent)
        columns.put("referer",referer)
        columns.put("browserName",browserName)
        columns.put("browserVersion",browserVersion)
        columns.put("osName",osName)
        columns.put("osVersion",osVersion)
        columns.put("formattime",formattime)

        // HBase api
        // HBase rowkey
        val rowkey = getRowKey(day,referer+url+ip+bytesent)
        val rk = Bytes.toBytes(rowkey)

        // put对象
        val put = new Put(Bytes.toBytes(rowkey))

        val list = new ListBuffer[((String,String),KeyValue)]()
        // 每一个rowkey对应的cf中的所有column字段
        for((k,v) <- columns) {
          val keyValue = new KeyValue(rk,"y".getBytes,Bytes.toBytes(k),Bytes.toBytes(v))
          list += (rowkey,k) ->keyValue
        }

        list.toList
      })
    }).sortByKey().map(x=>( new ImmutableBytesWritable(Bytes.toBytes(x._1._1)),x._2))

    val hbbaseConf = new Configuration()
    hbbaseConf.set("hbase.rootdir","hdfs://szgwnet01:9000/hbase")
    hbbaseConf.set("hbase.zookeeper.quorum","szgwnet01:2181,szgwnet02:2181,szgwnet03:2181")
    val tableName = createHbaseTable(day,hbbaseConf)

    // 设置写数据到哪个表中
    hbbaseConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)

   val job = NewAPIHadoopJob.getInstance(hbbaseConf)
   val htable = new HTable(hbbaseConf,tableName)
   HFileOutputFormat2.configureIncrementalLoad(job,htable.getTableDescriptor,htable.getRegionLocator)

   val output = "hdfs://szgwnet01:9000/test/access/v3"
   val outputPath = new Path(output)
   hbaseInfoRDD.saveAsNewAPIHadoopFile(
     output,
     classOf[ImmutableBytesWritable],
     classOf[KeyValue],
     classOf[HFileOutputFormat2],
     job.getConfiguration
   )

    if(FileSystem.get(hbbaseConf).exists(outputPath)){
      val load = new LoadIncrementalHFiles(job.getConfiguration)
      load.doBulkLoad(outputPath,htable)

      FileSystem.get(hbbaseConf).delete(outputPath,true)
    }

//    logDF.show(false)

    spark.stop()
  }

  def createHbaseTable(day:String,conf:Configuration) ={
    val table = "access_v3_" + day

    var connection:Connection = null
    var admin:Admin = null
    try {
      connection = ConnectionFactory.createConnection(conf)
      admin = connection.getAdmin

      /*
          这个Spark作业是离线的，一天运行一次，如果某天运行的时候出现问题
          重跑的时候，应该先把表数据删除，在重新写入
       */
      val tableName = TableName.valueOf(table)
      if(admin.tableExists(tableName)) {
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
      }

      val tabDesc = new HTableDescriptor(tableName)
      val famDesc = new HColumnDescriptor("y")
      tabDesc.addFamily(famDesc)
      admin.createTable(tabDesc)

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      if( null != admin ) {
        admin.close()
      }
      if ( null != connection ) {
        connection.close()
      }
    }
    table
  }

  def getRowKey(time:String,info:String) = {
    /**
      * rowkey是采用time_crc32(info) 拼接,time是当天时间
      * 只要是字符串拼接，尽量不要使用+ TODO...是一个非常经典的面试题(Java/Bigdata)
      *
      * StringBuffer  vs   StringBuilder
      */
    val builder = new StringBuilder(time)
    builder.append("_")

    val crc32 = new CRC32()
    crc32.reset()
    if(StringUtils.isNotEmpty(info)){
      crc32.update(Bytes.toBytes(info))
    }

    builder.append(crc32.getValue)

    builder.toString()
  }

  def flush(table:String,conf:Configuration): Unit ={

    var connection:Connection = null
    var admin:Admin = null
    try {
      connection = ConnectionFactory.createConnection(conf)
      admin = connection.getAdmin

      // memstore数据刷写到storefile中
      admin.flush(TableName.valueOf(table))

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      if( null != admin ) {
        admin.close()
      }
      if ( null != connection ) {
        connection.close()
      }
    }
  }
}