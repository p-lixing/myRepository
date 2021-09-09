package DemoUtils.flinkTest;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;


public class MainDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        Kafka kafka = new Kafka()
                .version("0.10")
                .topic("kafka")
                .property("bootstrap.servers", "localhost:9092");
        tableEnv.connect(kafka)
                .withFormat(
                        new Json().failOnMissingField(true).deriveSchema()
                )
                .withSchema(
                        new Schema()
                                .field("id", Types.INT)
                                .field("name", Types.STRING)
                                .field("sex", Types.STRING)
                                .field("score", Types.FLOAT)
                )
                .inAppendMode()
                .registerTableSource("tmp_table");
        String sql = "select * from tmp_table";
        Table table = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(table, Info.class).addSink(new MySQLWriter());
        env.execute();
    }
}
