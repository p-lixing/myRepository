package scalaDemo.com.demoTest.definedSource

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment,_}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

object kafkaSource {
  case class student(username: String ,age: Int)
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment =
      TableEnvironment.getTableEnvironment(env)
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

//    val myKafkaConsumer: FlinkKafkaConsumer011[String] = MyKafkaUtil.getConsumer("ECOMMERCE")
    val myKafkaConsumer = new FlinkKafkaConsumer010[String]("", new SimpleStringSchema(), props)
    val dstream: DataStream[String] = env.addSource(myKafkaConsumer)
    val dataDstream:DataStream[student]= dstream.map(data =>{
      val arr= data.split(" ")
      student(arr(0).toString,arr(1).toInt)
    })
    //方式1
    //常规创建 基于case class
//    tableEnv.fromDataStream(dataDstream)

    // Schema基于名称
//    val sensorTable = tableEnv.fromDataStream(dataDstream,'username as 'ts, 'id as 'myId)

    // Schema基于位置 一一对应
//    val sensorTable_lo = tableEnv.fromDataStream(dataDstream, "","")

    env.execute()
  }

}
