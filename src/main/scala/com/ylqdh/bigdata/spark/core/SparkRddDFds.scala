package com.ylqdh.bigdata.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkRddDFds {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkSqlJson").setMaster("local[*]")
    // 创建sparkSession对象
    val sparkSession:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


    // rdd,dataframe,dataset进行转换前要引入隐式转换规则
    // 这里sparkSession是上面创建的对象
    import sparkSession.implicits._

    val rdd = sparkSession.sparkContext.makeRDD(List((1,"sz","rainy"),(2,"gz","sunny"),(3,"dg","windy")))

    // 1. rdd->dataframe
    // rdd转换为dataframe需要添加属性
    val df = rdd.toDF("id","city","weather")

    // 2. dataframe -> rdd
    val rddf = df.rdd
    rddf.foreach(
      row => {
        println(row.getInt(0)+"\t"+row.getString(1)+"\t"+row.getString(2))
      }
    )

    // 3. dataframe -> dataset
    // 添加 类型
    val dfs = df.as[Weather]

    //rdd 到dataset需要添加属性和类型
    val weatherRDD = rdd.map{
      case (id,city,weather) => {
        Weather(id,city,weather)
      }
    }
    // 4. rdd->dataset
    val weatherDS = weatherRDD.toDS()

    // 5. dataset->rdd
    val rdds = weatherDS.rdd
    rdds.foreach(println)

    // 6. dataset -> dataframe
    val dsf = weatherDS.toDF()

    sparkSession.stop()
  }
}
case class Weather(id: Int, city: String, weather: String)