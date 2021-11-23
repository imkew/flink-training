package org.apche.flink.operators;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author kew
 * @Date 2021/11/23 10:57 下午
 **/
public class ReduceOperator {
    public static void main(String[] args) throws Exception {
        //获取环境变量
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        //准备数据,类型DataStreamSource
        DataStream<Tuple2<String,Integer>> source = senv.fromElements(new Tuple2<>("A",1),
                new Tuple2<>("B",3),
                new Tuple2<>("C",6),
                new Tuple2<>("A",5),
                new Tuple2<>("B",8));
        //使用reduce方法
        DataStream<Tuple2<String, Integer>> reduce  = source.keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2(value1.f0,value1.f1+value2.f1);
            }
        });

        reduce.print("reduce");
        senv.execute("Reduce Demo");
    }
}
