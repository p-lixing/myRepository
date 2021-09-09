package scalaDemo.com.SinkDemo.hbaseSink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * /usr/local/flink/bin/flink run -m yarn-cluster -ys 1 -ynm flinkTest -yn 1 -yjm 1024 -ytm 1024 -d -c DemoUtils.hbaseUtils.HBaseDemo filnkTest-1.0-SNAPSHOT.jar
 */

public class kafkaToHBaseDemo {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        FsStateBackend fsStateBackend = new FsStateBackend("hdfs://hdfsCluster/project/BDP/data/checkpointData");
        FsStateBackend fsStateBackend = new FsStateBackend("file:///D:\\data\\data\\flinkhbase");
//
        env.setStateBackend(fsStateBackend);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.enableCheckpointing(1 * 60 * 1000, CheckpointingMode.EXACTLY_ONCE);
//        //设置检查点超时间，如果在超时后，丢弃这个检查点,默认是10分钟
        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        //设置快照失败后任务继续正常执行
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
//        //设置并发检查点数量为1
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        //重跑策略
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.122.50:9092,172.16.122.55:9092,172.16.122.57:9092");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-hbase");
        //kafka分区自动发现周期
        prop.put(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "3000");

        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("flink_topic_test", new SimpleStringSchema(), prop);
        /*SimpleStringSchema可以获取到kafka消息，JSONKeyValueDeserializationSchema可以获取都消息的key,value，metadata:topic,partition，offset等信息*/
//        FlinkKafkaConsumer010<ObjectNode> kafkaConsumer011 = new FlinkKafkaConsumer010<>("topic_li_test2", new JSONKeyValueDeserializationSchema(true), prop);
        consumer.setStartFromEarliest();
        //source
        DataStream<String> source = env.addSource(consumer);
        DataStream<String> filterDs = source.filter((FilterFunction<String>) value -> value != null && value.length() > 0);
        filterDs.print();
        //用下面的方法批量Sink到HBase
        filterDs.addSink(new HBaseWriter());
        env.execute("sdsa");
    }
}

