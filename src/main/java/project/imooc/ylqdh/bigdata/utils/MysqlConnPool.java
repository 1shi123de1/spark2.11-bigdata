package project.imooc.ylqdh.bigdata.utils;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @ClassName MysqlConnPool
 * @Description TODO mysql c3p0 连接池
 * @Author ylqdh
 * @Date 2019/12/24 10:27
 */
public class MysqlConnPool {
    private static ComboPooledDataSource dataSource = new ComboPooledDataSource("mysql");

    private static Connection getConnection() {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
    public static void main(String[] args) {

    }
}

