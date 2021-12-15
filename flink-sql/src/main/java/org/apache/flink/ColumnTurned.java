package org.apache.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ColumnTurned {
    public static void main(String[] args) {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        /***
         * {"data":[{"id":"329523346276352","corp_id":"259995492941824","shop_id":"1","account_id":"329523346276352","init_amt":"1.0","cur_amt":"1.0","is_default":"0","remark":null,"status":"1","seq":"0","create_at":"2021-12-14 15:44:04","revise_at":"2021-12-14 15:44:04"},{"id":"329523346276353","corp_id":"259995492941824","shop_id":"310503964409856","account_id":"329523346276352","init_amt":"1.0","cur_amt":"1.0","is_default":"0","remark":null,"status":"1","seq":"0","create_at":"2021-12-14 15:44:04","revise_at":"2021-12-14 15:44:04"}]}
         */
        //kafka ods
        String ods_account_shop = "CREATE TABLE ods_account_shop (\n" +
                "    data ARRAY<ROW<id string,corp_id string,shop_id string,account_id string,init_amt string,cur_amt string,create_at string,revise_at string,remark string>>,\n" +
                "    pt as PROCTIME() \n" +
                ")\n" +
                "WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'ods_account_shop',\n" +
                "    'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092',\n" +
                "    'properties.group.id' = 'ods_account_shop',\n" +
                "    'format' = 'json',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'json.fail-on-missing-field' = 'false',\n" + //Whether to fail if a field is missing or not.
                "    'json.ignore-parse-errors' = 'true'\n" + //Skip fields and rows with parse errors instead of failing. Fields are set to null in case of errors.
                ")";


        tableEnv.executeSql(ods_account_shop);
        tableEnv.executeSql("select id ,corp_id ,shop_id ,init_amt from ods_account_shop, UNNEST(`data`) AS t (id ,corp_id ,shop_id ,account_id ,init_amt ,cur_amt ,create_at ,revise_at ,remark )").print();
    }
}
