import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

object SparkCoreHbase {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("hbase")
    val sc = new SparkContext(config)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "gwnet01,gwnet02,gwnet03")
    conf.set(TableInputFormat.INPUT_TABLE, "ylq:fruit")

    // get data
    val hbaseRDD = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    hbaseRDD.foreach {
      case (rowkey, result) => {
        val cells = result.rawCells()
        for (cell <- cells) {
          println(Bytes.toString(CellUtil.cloneValue(cell)))
        }
      }
    }

    // put data
        val dataRDD = sc.makeRDD(List(("1001","lightblue"),("1002","lightred")))
        val putRDD = dataRDD.map{
          case (rowkey,color)=>{
            val put = new Put(Bytes.toBytes(rowkey))
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("color"),Bytes.toBytes(color))

            (new ImmutableBytesWritable(Bytes.toBytes(rowkey)),put)
          }
        }
        val jobConf = new JobConf(conf)
        jobConf.setOutputFormat(classOf[TableOutputFormat])
        jobConf.set(TableOutputFormat.OUTPUT_TABLE,"ylq:fruit")

        putRDD.saveAsHadoopDataset(jobConf)

    sc.stop()
  }
}
