package project.imooc.ylqdh.bigdata.batch

import org.apache.spark.sql.SparkSession

/*
  测试Spark和HBase整合使用的兼容性
 */
object TestApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("testApp").master("local[*]").getOrCreate()

    val rdd = spark.sparkContext.parallelize(List(1,2,3,4))
    rdd.collect().foreach(println)

    spark.stop()
  }
}
