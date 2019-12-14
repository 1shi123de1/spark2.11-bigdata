

项目背景
        离线+实时   基于Spark(RDD/SQL/Streaming)
        基于慕课网的访问日志进行统计分析
            访问日志：离线   HBase
                点击流日志
                搜索
            订单数据日志：实时  Redis

            统计：不同需求/业务+根据不同的维度进行统计
                今天：新增了多少注册会员、订单量多少、订单金额多少
                今天和昨天对比：增长、减少？百分比
                会员
                订单
                运营商/地市


离线项目架构/处理流程
    1、数据采集(不涉及)：落地到HDFS  外部将数据采集到内部
        SDK数据==>日志==>Hadoop
        Server日志：flume、Logstash
        数据库：Sqoop
    2、数据预处理/数据清洗(*****)：脏/乱数据==>数据规整化  使用Spark
        时间解析
        加字段
            IP==>城市、运营商、维度
        减字段
        HDFS ==> Spark ==> HBase
    3、数据入库(*****)：把规整化的数据写入到存储 使用HBase
        Hive、HBase、Redis
        rowkey设计
    4、数据分析(*****)
        出报表的核心所在
        统计分析结果可以找个地方存储起来
        HBase ==> Mapreduce/Spark ==> 业务逻辑分析
        HBase ==> Hive/SparkSQL ==> SQL ==> DB
    5、数据展示(不涉及)：将分析得到的数据进行可视化显示
        可用技术：HUE、Zeppelin、Echarts、自研产品

 离线项目中要统计的指标/需求
    1）区域统计：国家、省份
    2）终端统计：浏览器、版本号
    目的 ==> Spark+HBase 综合运用 *****
    两个版本：
        业务逻辑实现
        各种性能的优化
    两种实现：
        Spark Core
        Spark SQL
        
        
  实时日志统计：见 [imooc项目实时流处理介绍] (https://github.com/1shi123de1/spark2.11-bigdata/blob/master/imooc%E9%A1%B9%E7%9B%AE%E5%AE%9E%E6%97%B6%E6%B5%81%E5%A4%84%E7%90%86%E4%BB%8B%E7%BB%8D.txt)
