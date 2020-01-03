package com.ylqdh.bigdata.scala.learn

import java.sql.{Connection, DriverManager, Timestamp}
import java.util.{Date, UUID}

import scala.util.Random

/* TODO 造500w 条数据，插入到Oracle COM_GWNET 表中 */
object ProducerOracle {

    def main(args: Array[String]): Unit = {

        val url = "jdbc:oracle:thin:@172.16.1.47:1521/myoracle"
        val driver = "oracle.jdbc.OracleDriver"
        val user = "c##futian"
        val passwd = "szgwnet"

        var connection:Connection = null

        try {
            Class.forName(driver)

            connection = DriverManager.getConnection(url,user,passwd)

            val sql = "insert into COM_GWNET(ID, COM_1, COM_2, COM_3, COM_4) VALUES (?,?,?,?,?)"
            val statement = connection.prepareStatement(sql)

            var i:Long = 99999
            while (i>0) {
                val id = UUID.randomUUID().toString
                val comm1 = List("hadoop","hive","spark","flink","kudu","sqoop","flume","impala","hbase","java","scala","yarn","zookeeper","hdfs","mapreduce","kafka","mysql","oracle","sqlserver","database","centos","github","maven","ubuntu","jdbc")
                val comm2 = Random.nextInt(1000000)+10000000
                val comm3 = getDate()
                val comm4 = Random.nextInt(100) + Random.nextDouble()
                println(id+"\t"+comm1(Random.nextInt(comm1.size))+"\t"+comm2+"\t"+comm3+"\t"+comm4.formatted("%.3f"))

                statement.setString(1,id)
                statement.setString(2,comm1(Random.nextInt(comm1.size)))
                statement.setInt(3,comm2)
                statement.setTimestamp(4,comm3)
                statement.setDouble(5,comm4.formatted("%.3f").toDouble)
                statement.addBatch()

                i = i-1

                // 10W条数据提交一次
                if(i%100000==0) {
                    statement.executeBatch()
                }
            }
        } catch {
            case e:Exception => e.printStackTrace()
        } finally {
            connection.close()
        }
    }

    def getDate():Timestamp = {
        val rd = Random.nextInt(100)
        val hs = Random.nextInt(24)
        val ms = Random.nextInt(60)
        val ss = Random.nextInt(60)

        val ts:Timestamp = new Timestamp(new Date().getTime + rd*hs*ms*ss*1000)

        return ts
    }
}
