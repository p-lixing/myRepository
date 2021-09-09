package scalaDemo.com.demoTest.definedSource

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment



object kafKaSorceTest2 {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // enable checkpoint
    val stateBackend = new FsStateBackend("file:///out/checkpoint")
    env.setStateBackend(stateBackend)
    env.enableCheckpointing(1 * 60 * 1000, CheckpointingMode.EXACTLY_ONCE)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "")
    props.put("group.id", "")
    props.put("auto.offset.reset", "latest") // latest/earliest
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "3000")

    // TBDS原生认证参数设置
    props.put("security.protocol", "SASL_TBDS")
    props.put("sasl.mechanism", "TBDS")
    props.put("sasl.tbds.secure.id", "")
    props.put("sasl.tbds.secure.key", "")

    val kafkaSource = new FlinkKafkaConsumer010[String]("kafka_offset", new SimpleStringSchema(), props)
    //        kafkaSource.setCommitOffsetsOnCheckpoints(false)

    val kafkaProducer = new FlinkKafkaProducer010[String]("kafka_offset_out", new SimpleStringSchema(), props)
    //        kafkaProducer.setWriteTimestampToKafka(true)

    env.addSource(kafkaSource)
      .setParallelism(1)
      .map(node => {
        node.toString + ",flinkx"
      })
      .addSink(kafkaProducer)

    // execute job
    env.execute("KafkaToKafka")


  }

}
