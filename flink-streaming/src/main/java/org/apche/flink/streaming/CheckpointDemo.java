package org.apche.flink.streaming;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * ./flink run -yid application_1648036096902_0019 -c org.apche.flink.streaming.CheckpointDemo
 * /home/hadoop/flink-streaming-1.0-SNAPSHOT.jar -openCheckPoint true -checkpointSavePath hdfs:///flink/chk
 *
 * -s 指定chk路径恢复作业
 */
public class CheckpointDemo {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointDemo.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        ParameterTool parameters = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameters);
        setup(env, parameters);
        // 设置并行度
        env.setParallelism(3);
        Properties source = new Properties();
        source.setProperty("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
        source.setProperty("group.id", "test");
        source.setProperty("auto.offset.reset", "latest");
        // 从kafka消费数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = env.addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), source))
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] s = value.split(" ");
                        for (String word : s) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                }).keyBy(0).sum(1);
        dataStream.print();

        //输出kafka
//        Properties sink = new Properties();
//        sink.setProperty("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
//        sink.setProperty("group.id", "wangketest");
//        sink.setProperty("auto.offset.reset", "latest");
//        dataStream.addSink(new FlinkKafkaProducer<Tuple2<String, Integer>>("wangketest", (KeyedSerializationSchema<Tuple2<String, Integer>>) new SimpleStringSchema(), sink));

        env.execute();
    }

    public static void setup(StreamExecutionEnvironment env, ParameterTool parameters) {
        //setup the checkpoint config
        try {
            boolean openCheckPoint = parameters.getBoolean("openCheckPoint", false);
            if (openCheckPoint) {
                CheckpointConfig config = env.getCheckpointConfig();
                String checkpointSavePath = parameters.get("checkpointSavePath");
                if (StringUtils.isNotBlank(checkpointSavePath)) {
                    StateBackend stateBackend = new RocksDBStateBackend(checkpointSavePath);
                    env.setStateBackend(stateBackend);
                }
                long checkpointInterval = parameters.getLong("checkpointInterval", 60 * 1000);
                String checkpointType = parameters.get("checkpointType", "AT_LEAST_ONCE");
                CheckpointingMode checkpointMode = CheckpointingMode.valueOf(checkpointType);
                LOG.warn("the checkpointMode is {}->{}", checkpointType, checkpointMode);
                long checkpointTimeout = parameters
                        .getLong("checkpointTimeout", checkpointInterval * 2 / 3);
                config.setCheckpointingMode(checkpointMode);
                config.setCheckpointInterval(checkpointInterval);
                config.setCheckpointTimeout(checkpointTimeout);
                config.setMinPauseBetweenCheckpoints(checkpointTimeout);
                //RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
                //DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
                env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(12, Time.of(10, TimeUnit.SECONDS)));
    }
}
