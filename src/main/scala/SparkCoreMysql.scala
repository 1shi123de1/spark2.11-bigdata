import java.sql.PreparedStatement

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkCoreMysql {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("mysql")

    val sc = new SparkContext(config)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://172.16.1.46:3306/test"
    val userName = "root"
    val passwd = "123456"

    // select
    val sql = "SELECT id, city, weather, `number`, spot FROM test.keya where id >=? and id <=?"
    val jdbcRDD = new JdbcRDD(
      sc,
      ()=>{
        Class.forName(driver)
        java.sql.DriverManager.getConnection(url,userName,passwd)
      },
      sql,
      1,
      6,
      2,
      (rs)=>{
        println(rs.getInt(1)+"  "+rs.getString(2)+"  "+rs.getString(3)+"  "+rs.getString(4)+"  "+rs.getString(5))
      }
    )
    jdbcRDD.collect()

    // insert
    val dataRDD = sc.makeRDD(List((100,"guangzhou","sunny","100","tianhe"),(101,"guangzhou","rainy","101","yuexiu")))
    dataRDD.foreachPartition(datas=> {
      Class.forName(driver)
      val connection = java.sql.DriverManager.getConnection(url, userName, passwd)
      datas.foreach {
          case (id, city, weather, number, spot) => {
          val sql = "insert into keya(id,city,weather,number,spot) values(?,?,?,?,?)"
          val statement: PreparedStatement = connection.prepareStatement(sql)
          statement.setInt(1, id)
          statement.setString(2, city)
          statement.setString(3, weather)
          statement.setString(4, number)
          statement.setString(5, spot)
          statement.executeUpdate()

          statement.close()
          connection.close()
        }
      }
    }
    )
    sc.stop()
  }
}
