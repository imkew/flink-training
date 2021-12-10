package org.apche.flink.streaming;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 生成最近半年连续的日期
 */
public class Date2Hbase {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.addSource(new SourceFunction<String>() {

            @Override
            public void run(SourceContext<String> ctx) throws Exception {

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                try {
                    // 起始日期
                    Date d1 = sdf.parse("2021-06-01");
                    // 结束日期
                    Date d2 = sdf.parse("2021-12-08");
                    Date tmp = d1;
                    Calendar dd = Calendar.getInstance();
                    dd.setTime(d1);
                    int count = 1 ;
                    while (tmp.getTime() < d2.getTime()) {
                        tmp = dd.getTime();
                        ctx.collect(sdf.format(tmp));
                        // 天数加上1
                        dd.add(Calendar.DAY_OF_MONTH, 1);
                        System.out.println(count++);
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void cancel() {
            }
        });
        dataStreamSource.print();
        dataStreamSource.addSink(new HbaseSink());
        env.execute();
    }

    public static class HbaseSink extends RichSinkFunction<String> {
        Connection conn = null;
        org.apache.hadoop.hbase.client.Table table = null;
        Put put = null;


        @Override
        public void open(Configuration parameters) throws Exception {
            org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
            configuration.set(HConstants.ZOOKEEPER_QUORUM, "kafka1");
            configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
            conn = ConnectionFactory.createConnection(configuration);
        }


        @Override
        public void invoke(String value, Context context) throws Exception {
            TableName recent_half_year_date = TableName.valueOf("recent_half_year_date");
            table = conn.getTable(recent_half_year_date);
            put = new Put(Bytes.toBytes(value));
            // 列簇，列名，列值
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("bill_date"), Bytes.toBytes(value));
            table.put(put);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

    }

}
