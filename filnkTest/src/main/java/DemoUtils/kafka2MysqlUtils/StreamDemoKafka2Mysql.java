package DemoUtils.kafka2MysqlUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class StreamDemoKafka2Mysql {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        FsStateBackend fsStateBackend = new FsStateBackend("hdfs://hdfsCluster/project/BDP/data/checkpointData");
        FsStateBackend fsStateBackend = new FsStateBackend("file:///D:\\data\\data\\flink");
//
        env.setStateBackend(fsStateBackend);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        env.enableCheckpointing(1 * 60 * 1000, CheckpointingMode.EXACTLY_ONCE);

        //重跑策略
        env.getConfig().setRestartStrategy(
                RestartStrategies.noRestart());
//        //设置检查点超时间，如果在超时后，丢弃这个检查点,默认是10分钟
        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        //设置快照失败后任务继续正常执行
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
//        //设置并发检查点数量为1
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.122.50:9092,172.16.122.55:9092,172.16.122.57:9092");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group2");
        //kafka分区自动发现周期
        prop.put(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "3000");

//        prop.put("auto.offset.reset", "earliest");// latest/earliest
//        prop.put("enable.auto.commit", "true");
//        prop.put("auto.commit.interval.ms", "3000");

        /*SimpleStringSchema可以获取到kafka消息，JSONKeyValueDeserializationSchema可以获取都消息的key,value，metadata:topic,partition，offset等信息*/
//        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("topic_li_test2", new SimpleStringSchema(), prop);
        FlinkKafkaConsumer010<ObjectNode> kafkaConsumer011 = new FlinkKafkaConsumer010<>("topic_li_test2", new JSONKeyValueDeserializationSchema(true), prop);
        kafkaConsumer011.setStartFromLatest();
        //加入kafka数据源
        DataStreamSource<ObjectNode> streamSource = env.addSource(kafkaConsumer011).setParallelism(1);
//        System.out.println("streamSource:" + streamSource.print());
        streamSource.print();
        SingleOutputStreamOperator<ObjectNode> filter = streamSource.filter((FilterFunction<ObjectNode>) value ->
                StringUtils.isNotBlank(value.toString()));
        //数据传输到下游
        filter.addSink(new MySqlTwoPhaseCommitSinkObject()).name("MySqlTwoPhaseCommitSinkBak");
        //触发执行
        env.execute("hh");
    }
}
