package project.weblog.ylqdh.bigdata.streaming

/**
  * 从搜索引擎过来的课程点击数 实体类
  * @param day_search_course
  * @param click_count
  */
case class CourseSearchCountCase(day_search_course:String,click_count:Long)
