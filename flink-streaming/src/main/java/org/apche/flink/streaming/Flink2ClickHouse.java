package org.apche.flink.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apche.flink.pojo.User;
import org.apche.flink.utils.ClickHouseUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: kewang
 * @Date: 2022/1/17 14:32
 */
public class Flink2ClickHouse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        List<User> userList = Arrays.asList(
                new User(001, "bj"),
                new User(002, "sz")
                );
        // source
        DataStream<String> inputStream = env.socketTextStream("localhost", 9999);
//        DataStreamSource<User> inputStream = env.fromCollection(userList);

        // Transform
        SingleOutputStreamOperator<User> dataStream = inputStream.map(new MapFunction<String, User>() {
            @Override
            public User map(String data) throws Exception {
                String[] split = data.split(",");
                return User.of(Integer.parseInt(split[0]),
                        split[1]
                );
            }
        });

        // sink
        String sql = "INSERT INTO test.t1 (id, name) VALUES (?,?)";
        MyClickHouseUtil jdbcSink = new MyClickHouseUtil(sql);
        dataStream.addSink(jdbcSink);
        dataStream.print();

        env.execute("clickhouse sink test");
    }

    public static class MyClickHouseUtil extends RichSinkFunction<User> {
        Connection connection = null;

        String sql;

        public MyClickHouseUtil(String sql) {
            this.sql = sql;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            connection = ClickHouseUtil.getConn("192.168.248.128", 8123, "test");
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (connection != null) {
                connection.close();
            }
        }

        @Override
        public void invoke(User user, Context context) throws Exception {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setLong(1, user.id);
            preparedStatement.setString(2, user.name);
            preparedStatement.addBatch();

            long startTime = System.currentTimeMillis();
            int[] ints = preparedStatement.executeBatch();
            connection.commit();
            long endTime = System.currentTimeMillis();
            System.out.println("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据 = " + ints.length);
        }
    }

}
