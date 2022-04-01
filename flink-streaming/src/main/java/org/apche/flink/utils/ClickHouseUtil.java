package org.apche.flink.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @Author: kewang
 * @Date: 2022/1/17 14:35
 */
public class ClickHouseUtil {
    private static Connection connection;

    public static Connection getConn(String host, int port, String database) throws SQLException, ClassNotFoundException {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String  address = "jdbc:clickhouse://" + host + ":" + port + "/" + database;
        connection = DriverManager.getConnection(address);
        return connection;
    }

    public static Connection getConn(String host, int port) throws SQLException, ClassNotFoundException {
        return getConn(host,port,"test");
    }
    public static Connection getConn() throws SQLException, ClassNotFoundException {
        return getConn("192.168.248.128",8123);
    }
    public void close() throws SQLException {
        connection.close();
    }
}
