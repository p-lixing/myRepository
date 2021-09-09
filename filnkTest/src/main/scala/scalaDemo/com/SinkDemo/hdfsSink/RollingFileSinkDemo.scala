package scalaDemo.com.SinkDemo.hdfsSink

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.formats.json.JsonNodeDeserializationSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
  * 使用BucketingSink 实现 根据‘数据’自定义输出目录
  */
object RollingFileSinkDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "172.16.122.50:9092,172.16.122.55:9092,172.16.122.57:9092")
    props.put("group.id", "flink_test_hdfs")

    //    props.put("auto.offset.reset", "latest") // latest/earliest
    //    props.put("enable.auto.commit", "true")
    //    props.put("auto.commit.interval.ms", "3000")

    // TBDS原生认证参数设置
    //    props.put("security.protocol", "SASL_TBDS")
    //    props.put("sasl.mechanism", "TBDS")
    //    props.put("sasl.tbds.secure.id", "")
    //    props.put("sasl.tbds.secure.key", "")


    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val source = new FlinkKafkaConsumer010[ObjectNode]("flink_topic_test", new JsonNodeDeserializationSchema, props)
    source.setStartFromGroupOffsets();
    val sink = new BucketingSink[String]("D:\\data\\data\\hadoop\\rollfilesink")
    sink.setBucketer(new DayBasePathBucketer)
    sink.setWriter(new StringWriter[String])

    // 下述两种条件满足其一时，创建新的块文件
    // 条件1.设置块大小为1MB
    sink.setBatchSize(1024 * 1024) // this is 1 MB,
//    sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    // 条件2.设置时间间隔60秒
        sink.setBatchRolloverInterval(60 * 1000) // this is 60秒//
    // 设置运行中的文件前缀
    //    sink.setInProgressPrefix("inProcessPre")
    // 设置块文件前缀
    //    sink.setPendingPrefix("pendingpre")
    //    sink.setPartPrefix("partPre")


    env.addSource(source)
      .assignAscendingTimestamps(json => {
        sdf.parse(json.get("date").asText()).getTime
      })
      .map(json => {
        json.get("date") + "-" + json.toString // 将日期拼接到前面，方便后面使用
      })
      .addSink(sink)

    env.execute("rollingFileSink")
  }

}
