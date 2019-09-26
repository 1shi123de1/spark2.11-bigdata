
_软件环境_：ubuntu16.04.5   flume 1.6.0      kafka_2.11-0.11.0.2   spark-2.1.1-bin-hadoop2.7   zookeeper-3.4.11   hadoop-2.7.7
 架构图
一共有4台服务器，服务器A上有flume，要监控服务器A上的/usr/local/flume/flume.log文件
服务器B、C、D组成集群，上面有Kafka、spark、zookeeper、hadoop，其中服务器B上也有一个flume。
读取文件数据的流程如下：服务器A上部署flume，读取文件，通过avro发送到服务器B上的flume；然后服务器B上的flume发送到集群的Kafka，Spark Streaming读取Kafka上的数据，并进行统计。

注：红色箭头为数据传输方向
   2.flume配置
服务器A、B上安装flume，下载解压apache-flume-1.6.0-bin.tar.gz后,修改
```
## 重命名flume-env.sh
mv flume-env.sh.template flume-env.sh
## 编辑flume-env.sh，设置JAVA_HOME属性
vim flume-env.sh
## export JAVA_HOME=/usr/java/jdk1.8.0_121/
服务器A的flume conf文件如下
mongoAgent.sources = sources1
mongoAgent.channels = channels1
mongoAgent.sinks = sinks1

## sources
mongoAgent.sources.sources1.type=exec
mongoAgent.sources.sources1.command=tail -f /usr/local/apache-flume-1.6.0-bin/testSpark.log

## channels
mongoAgent.channels.channels1.type=memory
mongoAgent.channels.channels1.capacity=1000
mongoAgent.channels.channels1.transactionCapacity=100

## sinks
mongoAgent.sinks.sinks1.type=avro
mongoAgent.sinks.sinks1.hostname=gwnet02
mongoAgent.sinks.sinks1.port=8989

## bind sources、channels、sinks
mongoAgent.sources.sources1.channels=channels1
mongoAgent.sinks.sinks1.channel=channels1
```
服务器B的flume conf配置如下：
```
kafkaAgent.sources = sources2
kafkaAgent.channels = channels2
kafkaAgent.sinks = sinks2

## sources
kafkaAgent.sources.sources2.type=avro
kafkaAgent.sources.sources2.bind=gwnet02
kafkaAgent.sources.sources2.port=8989

## channels
kafkaAgent.channels.channels2.type=memory
kafkaAgent.channels.channels2.capacity=1000
kafkaAgent.channels.channels2.transactionCapacity=100

## sinks
kafkaAgent.sinks.sinks2.type=org.apache.flume.sink.kafka.KafkaSink
kafkaAgent.sinks.sinks2.brokerList=szgwnet01:9092,szgwnet02:9092,szgwnet03:9092
kafkaAgent.sinks.sinks2.topic=testSpark
kafkaAgent.sinks.sinks2.batchSize=4
kafkaAgent.sinks.sinks2.requireAcks=1

## bind sources、channels、sinks
kafkaAgent.sources.sources2.channels=channels2
kafkaAgent.sinks.sinks2.channel=channels2
```

启动flume
先启动服务器B的flume agent，再启动服务器A的flume agent
```setsid flume-ng agent --conf ./conf --conf-file ./kafka-flume.conf --name kafkaAgent -Dflume.root.logger=DEBUG,console```
服务器A的flume agent启动，setsid是启动后在后台运行
```setsid ./bin/flume-ng agent --conf ./conf/ --name mongoAgent --conf-file conf/mongo-flume.conf -Dflume.root.logger=DEBUG,console```
3. Kafka配置
Kafka创建topic
```
## 在Kafka集群上创建topic
kafka-topics.sh --create --zookeeper szgwnet01:2181,szgwnet02:2181,szgwnet03:2181 --replication-factor 2 --partitions 1 --topic testSpark
## 在Kafka集群上启动一个消费者，后续用来观察数据(可选)
kafka-console-consumer.sh --zookeeper szgwnet01:2181,szgwnet02:2181,szgwnet03:2181 --topic testSpark --from-beginning
```

4.spark Streaming
spark Streaming编写scala代码，统计单词
(本次在IDEA中直接运行，没有打成jar包)
```
package com.ylqdh.bigdata.spark.streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
object ReadFromKafka {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("flumeKafkaSparkStreaming")
    // 实时数据分析环境对象
    // 采集周期：以指定的时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf,Seconds(5))
    // 从kafka中采集数据
    val kafkaDStream = KafkaUtils.createStream(
      streamingContext,
      "szgwnet01:2181,szgwnet02:2181,szgwnet03:2181",
      "ylqdhBigdata",
      Map("testSpark"->3)
    )
    // 将采集的数据进行分解(扁平化)
    val wordDStream = kafkaDStream.flatMap(t=>t._2.split(" "))
    // 将数据转换结构后方便统计分析
    val mapDStream = wordDStream.map((_,1))
    // 将数据进行聚合处理
    val wordSumDStream = mapDStream.reduceByKey(_+_)
    wordSumDStream.print()
    // 启动采集器
    streamingContext.start()
    // Driver等待采集器的执行
    streamingContext.awaitTermination()
  }
}
```
5、观察结果
  a.服务器A在文件中加入单词

b.Kafka消费者

c.同一时间，idea控制台中(idea运行的scala类没有手动停止)，Spark Streaming输入结果


后续改进1：把数据存进MySQL中，而不是打印在控制台
                2：spark Streaming的scala类打jar包在集群上运行
