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
      // 有两个参数，第二个参数默认是false，不shuffle，可以设置为true，进行shuffle
//      val coaRDD = sc.makeRDD(1 to 20 , 5)
//      coaRDD.saveAsTextFile("out")
//      val coaleaceRDD = coaRDD.coalesce(3)
//      println("缩减分区后："+coaleaceRDD.partitions.size)
//      coaleaceRDD.saveAsTextFile("output")

      // 11. repartition
      // 源码调用的是coalesce，第二个参数设置为true，coalesce(int , shuffle=true)
//      val reparRDD = sc.makeRDD(1 to 16 , 4)
//      val repartitionRDD  = reparRDD.repartition(2)
//      repartitionRDD.saveAsTextFile("output")

      // 12. sortBy
      // 按照指定的函数分区，第二个参数是升降序，true是升序
//      val sortByRDD = listRDD.sortBy(x=>x,false)
//      sortByRDD.collect().foreach(println)

      // 13.union
      // 两个RDD合并
//      val rdd1 = sc.makeRDD(1 to 5)
//      val rdd2 = sc.parallelize(4 to 8)
//      val unionRDD = rdd1.union(rdd2)
//      unionRDD.collect().foreach(println)

      // 14. substract
      // 计算两个RDD的差集,返回第一个RDD的差集
      // 如下两个RDD，只会返回RDD1的差集，如果反过来，则返回RDD2的差集
//      val rdd1 = sc.makeRDD(1 to 5)
//      val rdd2 = sc.parallelize(4 to 8)
//      val substractRDD = rdd1.subtract(rdd2)
//      substractRDD.collect().foreach(println)

      // 15. intersection
      // 返回两个RDD的交集
//      val rdd1 = sc.makeRDD(1 to 5)
//      val rdd2 = sc.parallelize(4 to 8)
//      val intersecRDD = rdd1.intersection(rdd2)
//      intersecRDD.collect().foreach(println)

      // 16.cartesian
      // 计算两个RDD的笛卡尔积，应尽量避免使用
//      val rdd1 = sc.makeRDD(2 to 5)
//      val rdd2 = sc.parallelize(5 to 8)
//      val carteRDD = rdd1.cartesian(rdd2)
//      carteRDD.collect().foreach(println)

      // 17. zip
      // 把两个value类型的RDD合并为一个K-V类型的RDD；注意两个RDD的分区及每个分区的数量要一致，否则报错
//      val rdd1 = sc.makeRDD(Array(3,4,5,6),4)
//      val rdd2 = sc.parallelize(Array("j","K","L","Q"),4)
//      val zipRDD = rdd1.zip(rdd2)
//      zipRDD.collect().foreach(println)

      // 18. partitionBy
      // 把KV类型RDD根据分区器分区，可自己重写分区器
//      val kvRDD = sc.parallelize(Array(("shan",1),("hang",2),("dang",3),("xing",4)),4)
//      val partiByRDD = kvRDD.partitionBy(new org.apache.spark.HashPartitioner(2))
//      partiByRDD.glom().collect().foreach(println)

      // 19. groupByKey
      // KV类型的RDD，根据key分组,后续可做聚合
//      val kvRDD = sc.parallelize(Array(("shan",1),("shan",2),("dang",3),("dang",4),("dang",1),("neng",5),("neng",1)))
//      val groupByKeyRDD = kvRDD.groupByKey()
//      groupByKeyRDD.collect().foreach(println)

      // 20. reduceByKey
      // KV类型RDD，根据key进行聚合
      // 第一个参数是聚合函数，第二个参数是任务数
//      val kvRDD = sc.parallelize(Array(("shan",1),("shan",2),("dang",3),("dang",4),("dang",1),("neng",5),("neng",1)))
//      val reduceByKeyRDD = kvRDD.reduceByKey(_+_,3)
//      reduceByKeyRDD.collect().foreach(println)

      /*
          groupByKey 和 reduceByKey的区别
          两个都要进行shuffle操作，所以过程都不会快
          但是reduceByKey 会进行预聚合，速度会比groupByKey快
       */

      // 21. aggregateByKey
      // 3个参数: 1是初始值，2是分区里的函数规则，3是分区间的函数规则
//      val aRDD = sc.parallelize(List(("a",3),("a",5),("c",6),("c",2),("b",6)),2)
//      aRDD.collect().foreach(println)
//      val aggRDD = aRDD.aggregateByKey(0)(math.max(_,_),_+_)
//      aggRDD.collect().foreach(println)

      // 22. foldByKey
      // 把相同key的值做函数运算；两个参数，第一个是初始值，第二个分区内和分区间的函数
//      val fRDD = sc.parallelize(List((1,3),(1,5),(2,6),(2,2),(3,6),(2,9)),3)
//      val foldRDD = fRDD.foldByKey(0)(_+_)
//      foldRDD.collect().foreach(println)

      // 23. combineByKey
      // 以下的例子是计算pairRDD的相同key值的平均值，第一个参数是把value变成(value,1)的形式，第二个参数是计算分区内的值，第三个参数是计算分区间的值
//      val cRDD = sc.parallelize(List(("a",3),("a",5),("c",6),("c",2),("b",6)),2)
//      val combineRDD = cRDD.combineByKey((_,1),(acc:(Int,Int),v)=>(acc._1+v,acc._2+1),(acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))
//      combineRDD.collect().foreach(println)

      // 24. sortByKey
      // 作用在pairRDD上，返回一个按照Key排序的RDD,参数false是降序，默认是升序
//      val sRDD = sc.parallelize(Array((2,"a"),(8,"r"),(5,"ww"),(5,"zz")))
//      val sortRDD = sRDD.sortByKey(false)
//      sortRDD.collect().foreach(println)

      // 25. mapValue
      // pairRDD ,只对value做操作
      // 例子对所有的value乘10
//      val mvRDD = sc.parallelize(List(("a",3),("a",5),("c",6),("c",2),("b",6)))
//      val mapvRDD = mvRDD.mapValues(_*10)
//      mapvRDD.collect().foreach(println)

      // 26. join
      // 两个pairRDD，相同的key值关联，value形成一个tuple；有笛卡尔积，性能较低
//      val joinRDD1 = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c"),(4,"d"),(5,"e")))
//      val joinRDD2 = sc.parallelize(Array((1,11),(2,222),(3,33),(4,44),(5,55)))
//      val joinRDD = joinRDD1.join(joinRDD2)
//      joinRDD.collect().foreach(println)

      // 27. cogroup
      //  相同的key值形成一个tuple，tuple里面的value是一个个的集合
      val coRDD1 = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c"),(4,"d"),(5,"e")))
      val coRDD2 = sc.parallelize(Array((1,11),(2,222),(3,33),(4,44),(5,55)))
      val coRDD = coRDD1.cogroup(coRDD2)
      coRDD.collect().foreach(println)
  }
}
