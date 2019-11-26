package project.weblog.ylqdh.bigdata.streaming

import com.ylqdh.bigdata.hbase.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 实战课程点击访数据问层
  */
object CourseClickCountDAO {

  val tableName = "weblog_course_click"
  val cf = "info"
  val quarifer = "click_count"

  /**
    * 保存数据到HBase
    * @param list CourseClickCount集合
    */
  def save(list:ListBuffer[CourseClickCountCase]) = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    for(ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(quarifer),
        ele.click_count)
    }
  }

  /**
    * 根据rowkey查询值
    * @param day_count
    * @return
    */
  def count(day_count:String) = {

    val table = HBaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(day_count))
    val value = table.get(get).getValue(cf.getBytes,quarifer.getBytes())

    if (value == null ) {
      0L
    }else {
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CourseClickCountCase]
    list.append(CourseClickCountCase("20191126_131",10))

//    save(list)
    println(count("20191126_121"))
    println(count("20191126_131"))

  }

}
