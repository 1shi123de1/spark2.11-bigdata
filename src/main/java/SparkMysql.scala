import org.apache.spark.{SparkConf, SparkContext}

object SparkMysql {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("mysql")

    val sc = new SparkContext(config)

    val driver = "com.mysql.jdbc.Deiver"
    val url = "jdbc:mysql://172.16.1.46:3306/test"
    val userName = "root"
    val passwd = "123456"



    sc.stop()
  }
}
