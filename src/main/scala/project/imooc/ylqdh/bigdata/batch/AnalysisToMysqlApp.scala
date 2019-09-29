package project.imooc.ylqdh.bigdata.batch

import java.sql.DriverManager

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

/**
  * 使用spark对HBase中的数据进行统计分析
  *   用了rdd的方式和DataFrame的方式
  *
  * 1.每个省份的访问量
  * 2.统计不同浏览器的访问量
  */
object AnalysisToMysqlApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                  .appName("AnalysisToMysqlApp").master("local[*]").getOrCreate()

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

    println("----------------------")


    // TODO...  2.统计浏览器的数量 top10
    val resultRDD = hbaseRDD.map(x=>{
      val browserName = Bytes.toString(x._2.getValue("y".getBytes,"browserName".getBytes))
      (browserName,1)
    }).reduceByKey(_+_)

    resultRDD.collect().foreach(println)

    // put result RDD into MYSQL
    //  可以试试用DataFrame、dataset的format("jdbc")方式写入MySQL
    resultRDD.foreachPartition(part => {
      Try{

        val connection ={
          val url = "jdbc:mysql://172.16.1.46:3306/test?characterEncoding=UTF-8"
          val user = "root"
          val password ="123456"
          Class.forName("com.mysql.jdbc.Driver")
          DriverManager.getConnection(url,user,password)
        }

        val preAutoCommit = connection.getAutoCommit
        connection.setAutoCommit(false)

        val sql = "insert into browser(day,name,cout) values(?,?,?)"
        val pstmt = connection.prepareStatement(sql)

        //  如果当天数据存在，则先删除
        pstmt.addBatch(s"delete from browser where day=$day")

        part.foreach(x=>{
          pstmt.setString(1,day)
          pstmt.setString(2,x._1)
          pstmt.setInt(3,x._2)

          pstmt.addBatch()
          pstmt.executeBatch()
          connection.commit()
        })

        (connection,preAutoCommit)
      }match {
        case Success((connection,preAutoCommit)) => {
          connection.setAutoCommit(preAutoCommit)
          if(null != connection) connection.close()
        }
        case Failure(e) => throw e
      }
    })


    //手动关闭rdd的cache
    hbaseRDD.unpersist()
    spark.stop()
  }

  case class ProvinceName(province:String)
  case class BrowserName(browserName:String)
}
