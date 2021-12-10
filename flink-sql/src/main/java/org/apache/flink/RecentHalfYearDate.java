package org.apache.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class RecentHalfYearDate {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        ParameterTool parameters = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameters);
        String start = parameters.get("start");
        String end = parameters.get("end");
        System.out.println(start+" "+end);


        //hbase
        String space_dim_company = "CREATE TABLE space_dim_company (\n" +
                " row_key STRING,\n" +
                " info ROW<id STRING,corp_id STRING>,\n" +
                " PRIMARY KEY (row_key) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'space_dim_company',\n" +
                " 'zookeeper.quorum' = 'kafka1:2181'\n" +
                ")";
        String recent_half_year_date = "CREATE TABLE recent_half_year_date (\n" +
                " row_key STRING,\n" +
                " info ROW<corp_id STRING,company_id STRING,bill_date STRING>,\n" +
                " PRIMARY KEY (row_key) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'recent_half_year_date',\n" +
                " 'zookeeper.quorum' = 'kafka1:2181'\n" +
                ")";


        tableEnv.executeSql(space_dim_company);
        tableEnv.executeSql(recent_half_year_date);
        tableEnv.createTemporarySystemFunction("produceDate",produceDate.class);

        tableEnv.executeSql("insert into recent_half_year_date select row_key,ROW(corp_id,company_id,bill_date) as info from (select concat(corp_id,id,bill_date)row_key,id company_id,corp_id,bill_date from space_dim_company ,lateral table(produceDate(id,corp_id,'"+start+"','"+end+"')))").print();


    }

    @FunctionHint(output = @DataTypeHint("ROW<s1 STRING, s2 STRING, bill_date STRING>"))
    public static class produceDate extends TableFunction<Row> {
        public void eval(String a, String b,String start,String end) {
            String bill_date;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            try {
                Date d1 = sdf.parse(start);
                Date d2 = sdf.parse(end);
                Date tmp = d1;
                Calendar dd = Calendar.getInstance();
                dd.setTime(d1);
                while (tmp.getTime() < d2.getTime()) {
                    tmp = dd.getTime();
                    bill_date = sdf.format(tmp);
                    collect(Row.of(a, b, bill_date));
                    dd.add(Calendar.DAY_OF_MONTH, 1);
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }
}

