package DemoUtils.hbaseUtils;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * /usr/local/flink/bin/flink run -m yarn-cluster -ys 1 -ynm flinkTest -yn 1 -yjm 1024 -ytm 1024 -d -c DemoUtils.hbaseUtils.HBaseDemo filnkTest-1.0-SNAPSHOT.jar
 */

public class HBaseDemo {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        FsStateBackend fsStateBackend = new FsStateBackend("hdfs://hdfsCluster/project/BDP/data/checkpointData");
        FsStateBackend fsStateBackend = new FsStateBackend("file:///D:\\data\\data\\flink");
//
        env.setStateBackend(fsStateBackend);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        env.enableCheckpointing(1 * 60 * 1000, CheckpointingMode.EXACTLY_ONCE);
//        //设置检查点超时间，如果在超时后，丢弃这个检查点,默认是10分钟
        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        //设置快照失败后任务继续正常执行
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
//        //设置并发检查点数量为1
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//失败重跑策略
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.122.50:9092,172.16.122.55:9092,172.16.122.57:9092");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group2");
        //kafka分区自动发现周期
        prop.put(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "3000");

//        prop.put("auto.offset.reset", "earliest");// latest/earliest
//        prop.put("enable.auto.commit", "true");
//        prop.put("auto.commit.interval.ms", "3000");
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("topic_li_test2", new SimpleStringSchema(), prop);
        /*SimpleStringSchema可以获取到kafka消息，JSONKeyValueDeserializationSchema可以获取都消息的key,value，metadata:topic,partition，offset等信息*/
//        FlinkKafkaConsumer010<ObjectNode> kafkaConsumer011 = new FlinkKafkaConsumer010<>("topic_li_test2", new JSONKeyValueDeserializationSchema(true), prop);

        consumer.setStartFromEarliest();
        consumer.setCommitOffsetsOnCheckpoints(true);
        //source
        DataStream<String> source = env.addSource(consumer);
        DataStream<String> filterDs = source.filter((FilterFunction<String>) value -> value != null && value.length() > 0);
        /*
        filterDs.map((MapFunction<String, List<Put>>) value -> {
                    List<Put> list = new ArrayList<>();
                    String[] args1 = value.split(",");

                    String rowKey = args1[0] + "_" + args1[1];
                    Put put = new Put(Bytes.toBytes(rowKey));
                    put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("column0"),Bytes.toBytes(args1[2]));
                    put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("column1"),Bytes.toBytes(args1[3]));
                    list.add(put);
                    return list;
                })
                .addSink(new HBaseSink());
        */

        //HBaseSink中入参是List<Put>，但是上面的实现只传一条数据，明显效率很低
        //用下面的方法批量Sink到HBase
        SingleOutputStreamOperator<List<Put>> apply = filterDs.countWindowAll(2).apply(new AllWindowFunction<String, List<Put>, GlobalWindow>() {
            @Override
            public void apply(GlobalWindow globalWindow, Iterable<String> iterable, Collector<List<Put>> collector) throws Exception {

                List<Put> putList1 = new ArrayList<>();
                for (String value : iterable) {
                    String[] columns = value.split(",");
                    String rowKey = columns[0] + "_" + columns[1];
                    Put put = new Put(Bytes.toBytes(rowKey));
                    for (int i = 2; i < columns.length; i++) {
                        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("column" + i), Bytes.toBytes(columns[i]));
                    }
                    putList1.add(put);
                }
                collector.collect(putList1);
            }
        });
//        SingleOutputStreamOperator<List<Put>> process = filterDs.countWindowAll(2).process(new ProcessAllWindowFunction<String, List<Put>, GlobalWindow>() {
//            @Override
//            public void process(Context context, Iterable<String> iterable, Collector<List<Put>> collector) throws Exception {
//                List<Put> putList1 = new ArrayList<>();
//                for (String value : iterable) {
//                    String[] columns = value.split(",");
//                    String rowKey = columns[0] + "_" + columns[1];
//                    Put put = new Put(Bytes.toBytes(rowKey));
//                    for (int i = 2; i < columns.length; i++) {
//                        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("column" + i), Bytes.toBytes(columns[i]));
//                    }
//                    putList1.add(put);
//                }
//                collector.collect(putList1);
//            }
//        });
        apply.addSink(new HBaseSink());
        apply.print();
        env.execute("sdsa");
    }
}

