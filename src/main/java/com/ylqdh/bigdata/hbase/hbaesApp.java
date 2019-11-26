package com.ylqdh.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class hbaesApp {

    private Connection connection = null;
    private Table table = null;
    private Admin admin = null;

    private String tableName = "weblog_course_click";

    @Before
    public void setUp() {
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","szgwnet01:2181,szgwnet02:2181,szgwnet03:2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();

            Assert.assertNotNull(connection);
            Assert.assertNotNull(admin);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getConnection(){

    }

    @Test
    public void createTable() throws IOException {
        TableName table = TableName.valueOf(tableName);
        if(admin.tableExists(table)){
            System.out.println("table is exist.");
        }else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
            hTableDescriptor.addFamily(new HColumnDescriptor("info"));
            hTableDescriptor.addFamily(new HColumnDescriptor("alarm"));

            admin.createTable(hTableDescriptor);

            System.out.println("table create success.");
        }
    }

    @Test
    public void listTables() throws IOException {
        HTableDescriptor[] tables = admin.listTables();
        if(tables.length > 0){
            for(HTableDescriptor table:tables){
                System.out.println(table.getNameAsString());

                HColumnDescriptor[] columnDescriptors = table.getColumnFamilies();
                for (HColumnDescriptor descriptor:columnDescriptors){
                    System.out.println("\t"+descriptor.getNameAsString());
                }

            }
        }
    }

    @Test
    public void testPut() throws IOException {
        table = connection.getTable(TableName.valueOf(tableName));

//        Put put = new Put(Bytes.toBytes("ylqdh"));
//
//        // put 设置数据的列族 rowkey value
//        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("province"),Bytes.toBytes("GuangDong"));
//        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("city"),Bytes.toBytes("SHENZHEN"));
//        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("country"),Bytes.toBytes("CN"));
//        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("zipCode"),Bytes.toBytes("518000"));

        List<Put> puts = new ArrayList<Put>();

        Put put1 = new Put(Bytes.toBytes("moon"));
        put1.addColumn(Bytes.toBytes("info"),Bytes.toBytes("province"),Bytes.toBytes("GuangDong"));
        put1.addColumn(Bytes.toBytes("info"),Bytes.toBytes("city"),Bytes.toBytes("SHENZHEN"));
        put1.addColumn(Bytes.toBytes("info"),Bytes.toBytes("country"),Bytes.toBytes("CN"));
        put1.addColumn(Bytes.toBytes("info"),Bytes.toBytes("zipCode"),Bytes.toBytes("518000"));

        Put put2 = new Put(Bytes.toBytes("star"));
        put2.addColumn(Bytes.toBytes("info"),Bytes.toBytes("province"),Bytes.toBytes("GuangDong"));
        put2.addColumn(Bytes.toBytes("info"),Bytes.toBytes("city"),Bytes.toBytes("SHENZHEN"));
        put2.addColumn(Bytes.toBytes("info"),Bytes.toBytes("country"),Bytes.toBytes("CN"));
        put2.addColumn(Bytes.toBytes("info"),Bytes.toBytes("zipCode"),Bytes.toBytes("518000"));

        puts.add(put1);
        puts.add(put2);

        table.put(puts);
    }

    @Test
    public void testUpdate() throws IOException {
        table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes("ylqdh"));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("city"),Bytes.toBytes("GUANGZHOU"));

        table.put(put);

    }

    @Test
    public void testGet01() throws IOException {
//        table = connection.getTable(TableName.valueOf(tableName));
        table = connection.getTable(TableName.valueOf("weblog_course_click"));

        Get get = new Get("20191126_121".getBytes());

        // get可以加cf，column
//        get.addFamily(Bytes.toBytes("info"));
//        get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("city"));

        Result result = table.get(get);
        printResult(result);
    }

    @Test
    public void testScan01() throws IOException {
        table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
//        Scan scan = new Scan(Bytes.toBytes("ylqdh"));   // 起始rowkey >=
//        Scan scan = new Scan(new Get(Bytes.toBytes("ylqdh")));  // 一个get对象
//        Scan scan = new Scan(Bytes.toBytes("ylqdh"),Bytes.toBytes("start")); // [start-rowkey stop-rowkey)

        // scan 添加列族、列信息
        scan.addFamily(Bytes.toBytes("info"));
        scan.addColumn(Bytes.toBytes("info"),Bytes.toBytes("city"));

        ResultScanner rs = table.getScanner(scan);

        // 直接在resultScanner中添加筛选信息
//        ResultScanner resultScanner = table.getScanner(Bytes.toBytes("info"),Bytes.toBytes("city"));

        for (Result result:rs){
            printResult(result);
            System.out.println("--------------------------------------------------");
        }
    }

    @Test
    public void testFilter01() throws IOException {
        table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        String reg = "^*oo*";

//        // rowkey  过滤器
//        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(reg));
//        // 前缀过滤
//        Filter filter1 = new PrefixFilter(Bytes.toBytes("ylq"));

        // 同时添加过个filter，参数可以设置filter间的关系
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ONE);

        Filter filter1 = new PrefixFilter("m".getBytes());
        Filter filter2 = new PrefixFilter("s".getBytes());

        filters.addFilter(filter1);
        filters.addFilter(filter2);

        scan.setFilter(filters);

        ResultScanner rs = table.getScanner(scan);
        for (Result result:rs){
            printResult(result);
            System.out.println("--------------------------------------------------");
        }
    }

    private void printResult(Result result){
        for (Cell cell:result.rawCells()){
            System.out.println(Bytes.toString(result.getRow())+"\t"
                    + Bytes.toString(CellUtil.cloneFamily(cell))+"\t"
                    + Bytes.toString(CellUtil.cloneQualifier(cell))+"\t"
                    + Bytes.toString(CellUtil.cloneValue(cell))+"\t"
                    + cell.getTimestamp());
        }
    }

    @After
    public void tearDown(){
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
