package project.weblog.ylqdh.bigdata.utils;

import org.apache.log4j.Logger;

/*
    模拟日志生成
 */
public class Log4jProduce {

    private static Logger logger = Logger.getLogger(Log4jProduce.class.getName());

    public static void main(String[] args) throws InterruptedException {
        int index =0;
        while (true) {
            Thread.sleep(1000);
            logger.info("value : " + index++);
        }

    }
}
