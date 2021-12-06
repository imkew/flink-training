package org.apche.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RankTets {
    public static void main(String[] args) {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String test = "CREATE TABLE test(\n" +
                "    company_id string,\n" +
                "    create_at string,\n" +
                "    bill_date string,\n" +
                "    end_amt bigint, \n" +
                "    pt as PROCTIME() \n" +
                ")\n" +
                "WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'test',\n" +
                "    'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092',\n" +
                "    'properties.group.id' = 'test',\n" +
                "    'format' = 'csv'\n" +
                ")";
        String wangketest = "CREATE TABLE wangketest(\n" +
                "    rk bigint,\n" +
                "    company_id string,\n" +
                "    create_at string,\n" +
                "    bill_date string,\n" +
                "    end_amt bigint, \n" +
                "    PRIMARY KEY (rk) NOT ENFORCED \n" +
                ")\n" +
                "WITH (\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'wangketest',\n" +
                "    'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092',\n" +
                "    'properties.group.id' = 'wangketest',\n" +
                "    'key.format' = 'csv', \n" +
                "    'value.format' = 'csv' \n" +
                ")";


        tableEnv.executeSql(test);
        tableEnv.executeSql(wangketest);
        tableEnv.executeSql("insert into wangketest select company_id,create_at,bill_date,end_amt,rk from (select *,row_number()over(order by bill_date,create_at) as rk from test) where rk < 100").print();
    }
}
