package DemoUtils.flinkTest;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class MysqlSinkTest {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FsStateBackend fsStateBackend = new FsStateBackend("file:///D:\\data\\data\\flink2");
        env.setStateBackend(fsStateBackend);
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.122.50:9092,172.16.122.55:9092,172.16.122.57:9092");
        properties.setProperty("group.id", "scalaDemo");
        // 1,abc,100 类似这样的数据，当然也可以是很复杂的json数据，去做解析
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("topic_li_test2", new SimpleStringSchema(), properties);
//        consumer.setStartFromEarliest();     // start from the earliest record possible
        consumer.setStartFromLatest();       // start from the latest record
//        consumer.setStartFromGroupOffsets();
        env.getConfig().disableSysoutLogging(); //设置此可以屏蔽掉日记打印情况
        env.getConfig().setRestartStrategy(
                RestartStrategies.fixedDelayRestart(5, 5000));
        env.enableCheckpointing(20000);// 每2秒做一次checkpoint
        DataStream<String> stream = env
                .addSource(consumer);
        DataStream<Tuple3<Integer, String, Integer>> sourceStream = stream.filter((FilterFunction<String>) value ->
                StringUtils.isNotBlank(value))
                .map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {
                    @Override
                    public Tuple3<Integer, String, Integer> map(String value) throws Exception {
                        String[] args1 = value.split(",");
                        return new Tuple3<Integer, String, Integer>(Integer
                                .valueOf(args1[0]), args1[1], Integer
                                .valueOf(args1[2]));
                    }
                });
//                .map((MapFunction<String, Tuple3<Integer, String, Integer>>) value -> {
//                    String[] args1 = value.split(",");
//                    return new Tuple3<Integer, String, Integer>(Integer
//                            .valueOf(args1[0]), args1[1],Integer
//                            .valueOf(args1[2]));
//                });


        sourceStream.addSink(new MySqlTwoPhaseCommitSink());
//        sourceStream.addSink(new MysqlSink());
        env.execute("data to mysql start");
    }
}
