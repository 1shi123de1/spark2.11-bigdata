package project.weblog.ylqdh.bigdata.streaming

import com.ylqdh.bigdata.hbase.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 实战课程点击访数据问层
  */
object CourseSearchCountDAO {

  val tableName = "weblog_course_search"
  val cf = "info"
  val quarifer = "click_count"

  /**
    * 保存数据到HBase
    * @param list CourseClickCount集合
    */
  def save(list:ListBuffer[CourseSearchCountCase]) = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    for(ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_search_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(quarifer),
        ele.click_count)
    }
  }

  /**
    * 根据rowkey查询值
    * @param day_search_count
    * @return
    */
  def count(day_search_count:String) = {

    val table = HBaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(day_search_count))
    val value = table.get(get).getValue(cf.getBytes,quarifer.getBytes())

    if (value == null ) {
      0L
    }else {
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CourseSearchCountCase]
    list.append(CourseSearchCountCase("20191126_www.baidu.com_111",10))
    list.append(CourseSearchCountCase("20191126_www.sogou.com_111",12))

    save(list)
    println(count("20191126_www.baidu.com_111"))
    println(count("20191126_www.sogou.com_111"))

  }

}
