import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperators {
  def main(args: Array[String]): Unit = {
    // 创建config，指定本地运行模式，并设置App的name
    val config:SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkOperator")
    val sc = new SparkContext(config)

    // 自定义，从数组生成RDD
    val listRDD = sc.makeRDD(1 to 10)

    // 1. map 操作
    // 有多少个元素，就发送多少次到执行器(Excutor)
//    val mapRDD = listRDD.map(_*2)
//    mapRDD.collect().foreach(println)

    // 2. mapPartitions
    // 一个分区发送一次到excutor，效率优于map算子，减少了发送到执行器(Excutor)执行交互次数
    // 由于一次发送一个partition，可能内存溢出，oom错误
//    val mapPartitionsRDD = listRDD.mapPartitions(datas=>{
//      datas.map(_*2)
//    })
//    mapPartitionsRDD.collect().foreach(println)


      // 3. mapPartitionsWtihIndex
      // 可以形成分区号，后续可根据分区号进行操作
//      val tupleRDD = listRDD.mapPartitionsWithIndex{
//        case(num,datas)=>{
//          datas.map((_,"分区号："+num))
//        }
//      }
//      tupleRDD.collect().foreach(println)


      // 4. flatMap
      // 扁平化，把一个KV类型的数据转换为 一个List或Array
//      val list = sc.makeRDD(Array(List(1,2),List(3,4)))
//      val flatMapRDD = list.flatMap(data=>data)
//      flatMapRDD.collect().foreach(println)

      // 5. glom
      // 将一个分区的数据放到一个数组中
//      val glomList = sc.makeRDD(1 to 24 , 4)
//      val glomRDD = glomList.glom()
//      glomRDD.collect().foreach(array=>{
//        println(array.mkString(","))
//      })

      // 6. groupBy(Func)
      // 按照传入的函数返回值分组，将相同的key对应的值放入一个迭代器
      // 分组后的数据形成了对偶元组(K-V),K是分组的key，V是分组后的数据集合
//      val groupRDD = listRDD.groupBy(i=>i%2)
//      groupRDD.collect().foreach(println)

      // 7. filter
      // 按照指定的规则过滤数据
//      val filterRDD=listRDD.filter(x=>x%2==0)
//      filterRDD.collect().foreach(println)

      // 8. sample
      // 按照指定的随机种子随机抽样数据
      // 三个参数，第一个参数true/false，true表示有放回的抽，false表示无放回的抽
      //  第二个参数是打分高低，第三个是种子
//      val sampleRDD = listRDD.sample(false,0.4,123)
//      sampleRDD.collect().foreach(println)

      // 9. distinct
      // 数据去重，会有shuffle的过程，去重后会导致数据的减少，所以可以改变默认的分区数
//      val distinctRDD = listRDD.distinct(2)
//      distinctRDD.collect().foreach(println)
//      distinctRDD.saveAsTextFile("output")

      // 10. coalesce
      // 缩减分区数,第一个分区不变，把剩下的分区合并，没有shuffle
      val coaRDD = sc.makeRDD(1 to 20 , 5)
      coaRDD.saveAsTextFile("out")
      val coaleaceRDD = coaRDD.coalesce(3)
      println("缩减分区后："+coaleaceRDD.partitions.size)
      coaleaceRDD.saveAsTextFile("output")

  }
}
