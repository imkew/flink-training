package org.apache.flink.connectors.socket;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: kewang
 * @Date: 2022/4/13 13:36
 * <p>
 * nc -lk 9999
 * INSERT|Alice|12
 * INSERT|Bob|5
 * DELETE|Alice|12
 * INSERT|Alice|18
 */
public final class ChangelogSocketExample {

    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String hostname = params.get("hostname", "localhost");
        final String port = params.get("port", "9999");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // source only supports parallelism of 1

        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // register a table in the catalog
        tableEnv.executeSql(
                "CREATE TABLE UserScores (name STRING, score INT)\n"
                        + "WITH (\n"
                        + "  'connector' = 'socket',\n"
                        + "  'hostname' = '"
                        + hostname
                        + "',\n"
                        + "  'port' = '"
                        + port
                        + "',\n"
                        + "  'byte-delimiter' = '10',\n"
                        + "  'format' = 'changelog-csv',\n"
                        + "  'changelog-csv.column-delimiter' = '|'\n"
                        + ")");
        tableEnv.executeSql("SELECT * FROM UserScores").print();
    }
}
