package org.apache.flink.connectors.mongodb;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: kewang
 * @Date: 2022/4/18 16:03
 */
public class MongodbExample {
    public static void main(String args[]) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        String sourceSql = "CREATE TABLE datagen (\n" +
                " id INT,\n" +
                " name STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.name.length'='10'\n" +
                ")";
        String sinkSql = "CREATE TABLE mongoddb (\n" +
                "  id INT,\n" +
                "  name STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'mongodb',\n" +
                "  'database'='articledb',\n" +
                "  'collection'='comment',\n" +
                "  'uri'='mongodb://kk:123456@xxxxxx:27017/?authSource=articledb',\n" +
                "  'maxConnectionIdleTime'='20000',\n" +
                "  'batchSize'='3'\n" +
                ")";
        String insertSql = "insert into mongoddb " +
                "select id,name " +
                "from datagen";


        tableEnvironment.executeSql(sourceSql);
        tableEnvironment.executeSql(sinkSql);
        tableEnvironment.executeSql(insertSql);
//        tableEnvironment.executeSql("select * from datagen").print();

    }
}
