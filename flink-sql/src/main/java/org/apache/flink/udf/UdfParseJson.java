package org.apache.flink.udf;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.hbase.thirdparty.com.google.gson.JsonElement;
import org.apache.hbase.thirdparty.com.google.gson.JsonObject;
import org.apache.hbase.thirdparty.com.google.gson.JsonParser;
import org.apache.log4j.Logger;

/**
 * @Author: kewang
 * @Date: 2022/4/1 13:57
 */
public class UdfParseJson {
    public static final Logger logger = Logger.getLogger(UdfParseJson.class);

    /**
     * 主要用来解析message字段里的嵌套json
     * 样例数据：
     * {
     *     "@timestamp":1648793592.36719,
     *     "log_time":"2022-04-01 14:13:12,366",
     *     "level":"INFO",
     *     "midmsg":"reactor-http-epoll-3 [TID: N/A] c.z.g.r.s.i.GatewayLogRecordServiceImpl 156:",
     *     "message":"{\"uid\":389631942066176,\"corp-id\":389632999030784,\"method\":\"POST\"}"
     * }
     *
     * 输出：
     * +----+--------------------------------+--------------------------------+--------------------------------+
     * | op |                       log_time |                            uid |                        corp_id |
     * +----+--------------------------------+--------------------------------+--------------------------------+
     * | +I |                     2022-04-01 |                389665773322240 |                389666360524800 |
     *
     * @param args
     */
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        String space_gateway_log = "CREATE TABLE space_gateway_log (\n" +
                "    log_time string,\n" +
                "    level string ,\n" +
                "    message string\n" +
                ")\n" +
                "WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'space_gateway_log',\n" +
                "    'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092',\n" +
                "    'properties.group.id' = 'space_gateway_log ',\n" +
                "    'format' = 'json',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" + //'earliest-offset', 'latest-offset'
                "    'json.fail-on-missing-field' = 'false',\n" + //Whether to fail if a field is missing or not.
                "    'json.ignore-parse-errors' = 'true'\n" + //Skip fields and rows with parse errors instead of failing. Fields are set to null in case of errors.
                ")";
        tableEnv.executeSql(space_gateway_log);
        // 注册函数
        tableEnv.createTemporarySystemFunction("udf_parse_json",ParseJson.class);
        tableEnv.executeSql("\n" +
                "SELECT substring(log_time,0,10) log_time,data[1] uid,data[2] corp_id\n" +
                "FROM space_gateway_log\n" +
                "LEFT JOIN LATERAL TABLE(udf_parse_json(message, 'uid', 'corp-id')) AS T(data) ON TRUE").print();
    }

    @FunctionHint(output = @DataTypeHint("ROW<arr ARRAY<STRING>>"))
    public static class ParseJson extends TableFunction<Row> {
        public void eval(String... json) {
            try {
                if (json == null || json.length == 0 || json[0] == null) {
                    return;
                }
                String[] arr = getStrings(json);
                RowKind rowKind = RowKind.fromByteValue((byte) 0);
                Row row = new Row(rowKind, 1);
                row.setField(0, arr);
                collect(row);
            }catch (Exception e){
                logger.error("json parse fail, may be have useless data", e);
            }
        }

        public String[] getStrings(String[] json) {
            JsonObject jsonObject = new JsonParser().parse(json[0]).getAsJsonObject();
            int len = json.length - 1;
            String[] arr = new String[len];
            for (int i = 0; i < len; ++i) {
                JsonElement tm = jsonObject.get(json[i + 1]);
                if (tm != null) {
                    arr[i] = tm.getAsString();
                } else {
                    arr[i] = null;
                }
            }
            return arr;
        }
    }
}
