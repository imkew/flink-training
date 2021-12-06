package org.apche.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class leadDemo {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String wangketest = "CREATE TABLE wangketest(\n" +
                "    company_id string,\n" +
                "    bill_date string,\n" +
                "    end_amt string,\n" +
                "    pt as PROCTIME() \n" +
                ")\n" +
                "WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'wangketest',\n" +
                "    'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092',\n" +
                "    'properties.group.id' = 'wangketest',\n" +
                "    'format' = 'csv',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" + //latest earliest
                "    'csv.ignore-parse-errors' = 'true'\n" + //Whether to fail if a field is missing or not.
                ")";
        String space_dwd_fund_pur_company_adjust_union_hbase = "CREATE TABLE space_dwd_fund_pur_company_adjust_union_hbase (\n" +
                " row_key STRING,\n" +
                " info ROW<company_id STRING,code STRING,total_amt STRING,create_at STRING,bill_date STRING,tag STRING,end_amt STRING>,\n" +
                " PRIMARY KEY (row_key) NOT ENFORCED,\n" +
                " pt as PROCTIME() \n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'space_dwd_fund_pur_company_adjust_union',\n" +
                " 'zookeeper.quorum' = 'zookeeper1:2181,zookeeper2:2181,zookeeper3:2181'\n" +
                ")";

        tableEnv.executeSql(wangketest);
        tableEnv.executeSql(space_dwd_fund_pur_company_adjust_union_hbase);
        tableEnv.executeSql("select company_id,bill_date,end_amt,lag(end_amt)over(partition by company_id,bill_date order by pt) last_end_amt from (select pt,company_id,bill_date,sum(cast(total_amt as int))over(partition by company_id,bill_date order by pt) end_amt from space_dwd_fund_pur_company_adjust_union_hbase)").print();
    }
}
