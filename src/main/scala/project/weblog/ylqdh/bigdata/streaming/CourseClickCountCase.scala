package project.weblog.ylqdh.bigdata.streaming

/**
  * 课程点击数实体类
  * @param day_course   天加课程号，对应的是HBase中的rowkey，20191125_131
  * @param click_count  20191125_131对应的访问总数
  */
case class CourseClickCountCase (day_course:String,click_count:Long)
