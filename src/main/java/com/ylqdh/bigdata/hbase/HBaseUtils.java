package com.ylqdh.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase 操作工具类
 * java 工具类建议采用单例模式封装
 * 工具类中的get和put函数要配套使用
 */
public class HBaseUtils {
    Admin admin = null;
    Configuration configuration = null;

    Connection connection = null;

    private HBaseUtils() {
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","szgwnet01:2181,szgwnet02:2181,szgwnet03:2181");
        configuration.set("hbase.rootdir","hdfs://szgwnet01:9000/hbase");

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance = null;
    public static synchronized HBaseUtils getInstance() {
        if (null == instance) {
            instance = new HBaseUtils();
        }
        return instance;
    }

    public Table getTable(String tableName) {
        Table table = null;

        try{
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;
    }

    /**
     * 插入一条数据到hbase中
     * @param tableName   要插入的表名
     * @param rowkey      要插入的表rowkey
     * @param cf          要插入的表列族
     * @param column      要插入的表列
     * @param value       要插入的值
     */
    public void put(String tableName,String rowkey,String cf,String column,String value) {
        Table table = HBaseUtils.getInstance().getTable(tableName);

        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *  获取hbase表某个列的单个值
     *  注意，函数返回值是string类型
     * @param tableName     要获取值的表名
     * @param rowkey        要获取值的表rowkey
     * @param cf            要获取值的表列族
     * @param column        要获取值的表列
     */
    public String get (String tableName,String rowkey,String cf,String column) {
        Table table = HBaseUtils.getInstance().getTable(tableName);

        Get get = new Get(Bytes.toBytes(rowkey));

        String value = null;
        try {
            for (Cell cell : table.get(get).rawCells()) {
                value = Bytes.toString(CellUtil.cloneValue(cell));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return value;
    }

    public static void main(String[] args) {
//        HBaseUtils.getInstance().put("weblog_course_click","20191125_111","info","click_count","5");
        System.out.println(HBaseUtils.getInstance().get("weblog_course_click","20191125_111","info","click_count"));
//        System.out.println(HBaseUtils.getInstance().get("weblog_course_click","20191125_131","info","click_count"));

    }

}
