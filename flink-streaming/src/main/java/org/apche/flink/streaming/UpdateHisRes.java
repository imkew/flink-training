package org.apche.flink.streaming;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apche.flink.pojo.BillFund;
import org.apche.flink.pojo.BillFundEnd;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

/**
 * @Author kew
 * @Date 2021/11/7 1:42 下午
 **/
public class UpdateHisRes {
    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度 = 1
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka1:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        // 从kafka消费数据
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties));
        // 转换成BillFund类型
        DataStream<BillFund> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new BillFund(new String(fields[0]), new Integer(fields[1]), new Integer(fields[2]), new Integer(fields[3]));
        });
        // 定义一个OutputTag，用来表示侧输出流
        // An OutputTag must always be an anonymous inner class
        // so that Flink can derive a TypeInformation for the generic type parameter.
        OutputTag<BillFund> sideStream = new OutputTag<BillFund>("lowTemp") {
        };
        // ProcessFunction，自定义侧输出流实现分流操作
        SingleOutputStreamOperator<BillFundEnd> mainStream = dataStream
                .keyBy(BillFund::getF1)
                .process(new ProcessFunction<BillFund, BillFundEnd>() {
                    private transient ValueState<BillFundEnd> end_amt;
                    int count = 0;

                    @Override
                    public void open(Configuration config) {
                        ValueStateDescriptor<BillFundEnd> descriptor =
                                new ValueStateDescriptor<>(
                                        "end_amt", // the state name
                                        TypeInformation.of(new TypeHint<BillFundEnd>() {
                                        }) // type information
                                ); // default value of the state, if nothing was set
                        end_amt = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(BillFund value, Context ctx, Collector<BillFundEnd> out) throws Exception {
                        // 判断，bill_date=create_at，输出到主流，不等于输出到侧输出流
                        if (value.getF2() != value.getF3()) {
                            ctx.output(sideStream, value);
                        }
                        // access the state value
                        BillFundEnd billFundEnd = end_amt.value();
                        // update the count
                        count++;
                        // init state
                        if (billFundEnd == null) {
                            System.out.println("init state billFundEnd:" + billFundEnd);
                            billFundEnd = new BillFundEnd(value.f1, value.f2, value.f3, value.f4, 0);
                        }
                        // add the second field of the input value
                        System.out.println("当前账单结余金额=" + billFundEnd.f5 + "\n" + "当前付款收款调整金额=" + value.f4);
                        billFundEnd.f5 += value.f4;
                        out.collect(new BillFundEnd(value.f1, value.f2, value.f3, value.f4, billFundEnd.f5));
                        // update the state
                        end_amt.update(billFundEnd);
                        System.out.println("状态缓存的最新账单余额为：" + billFundEnd.f5);
                        // if the count reaches 100, emit the average and clear the state
                        if (count > 100) {
                            // 生产环境需要设置ttl清理状态数据
                            end_amt.clear();
                        }
                        System.out.println("======================================================================");
                    }
                });

        DataStream<BillFund> sideOutput = mainStream.getSideOutput(sideStream);
        sideOutput.print("侧流");
        mainStream.print("主流");
        mainStream.addSink(new MyJdbcSink());
        env.execute();
    }

    // 实现自定义的SinkFunction
    public static class MyJdbcSink extends RichSinkFunction<BillFundEnd> {
        // 声明连接和预编译语句
        Connection connection = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;
        PreparedStatement selectStmt = null;
        PreparedStatement updateStmtCurr = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/demo?useUnicode=true&serverTimezone=Asia/Shanghai&characterEncoding=UTF-8&useSSL=false", "root", "root");
            insertStmt = connection.prepareStatement("insert into billEnd (f1, f2, f3, f4, f5) values (?, ?, ?, ?, ?)");
            selectStmt = connection.prepareStatement("select (f5-f4) from billEnd where f3 > ? limit 1");
            updateStmtCurr = connection.prepareStatement("update billEnd set f5 = ?+? where f3 = ?");
            updateStmt = connection.prepareStatement("update billEnd set f5 = f5+? where f3 > ?");
        }

        // 每来一条数据，调用连接，执行sql
        @Override
        public void invoke(BillFundEnd value, Context context) throws Exception {

            insertStmt.setString(1, value.getF1());
            insertStmt.setInt(2, value.getF2());
            insertStmt.setInt(3, value.getF3());
            insertStmt.setInt(4, value.getF4());
            insertStmt.setInt(5, value.getF5());
            insertStmt.execute();

            if (value.f2 != value.f3) {
                System.out.println("开始处理异常数据...");
                int late_date = value.f3;
                int late_amt = value.f4;

                selectStmt.setInt(1, late_date);
                int init_amt = 0;
                ResultSet resultSet = selectStmt.executeQuery();
                while (resultSet.next()) {
                    init_amt = resultSet.getInt(1);
                }
                System.out.println("异常数据上一条账单的结余金额:" + init_amt);
                updateStmtCurr.setInt(1,init_amt);
                updateStmtCurr.setInt(2,late_amt);
                updateStmtCurr.setInt(3,late_date);
                updateStmtCurr.execute();

                updateStmt.setInt(1, late_amt);
                updateStmt.setInt(2, late_date);
                updateStmt.execute();
                System.out.println("异常数据处理完成...");
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            selectStmt.close();
            updateStmtCurr.close();
            connection.close();
        }
    }
}
