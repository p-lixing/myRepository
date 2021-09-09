package scalaDemo.com.demoTest.waterMarkUtils

import org.apache.flink.api.common.functions.{RichFilterFunction, RichFlatMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import scalaDemo.com.demoTest.definedSource.MySensorSource

object Sensor {

  // 定义样例类，传感器 id，时间戳，温度
  case class SensorReading(id: String, timestamp: Long, temperature: Double)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream4 = env.addSource(new MySensorSource())
    val stream1 = env
      .fromCollection(List(
        SensorReading("sensor_1", 1627613049, 35.80018327300259),
        SensorReading("sensor_6", 1627613075, 15.402984393403084),
        SensorReading("sensor_6", 1627613075, 14.402984393403084),
        SensorReading("sensor_7", 1627613082, 6.720945201171228),
        SensorReading("sensor_10", 1627613093, 38.101067604893444)
      ))
    // event time 的引入
//      .assignAscendingTimestamps(t=>t.timestamp)
    val stream2 = env.fromCollection(List(1,2,3,4,5,6))
    //        stream1.print("stream1:").setParallelism(1)

        val minTempPerWindow = stream1
          .map(r => (r.id, r.temperature))
          .keyBy(_._1)
          .timeWindow(Time.seconds(3))
          .reduce((r1, r2) => (r1._1, r1._2.min(r2._2))).print()

    //split和select算子
    val splitStream = stream1
      .split(sensorData => {
        if (sensorData.temperature > 30) Seq("high") else Seq("low")
      })
    val high = splitStream.select("high")
    val low = splitStream.select("low")
    val all = splitStream.select("high", "low")

    val warning = high.map(sensorData => (sensorData.id,
      sensorData.temperature))
    val connected = warning.connect(low)
    val coMap = connected.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy"))
//    coMap.print()


    //自定义UDF，匿名类,匿名函数,富函数
    val flinkTweets = high.filter(
      new RichFilterFunction[SensorReading] {
        override def filter(value: SensorReading): Boolean = {
          value.id.equals("sensor_1")
        }
      })
    val flinkTweets2 = high.filter(_.id.contains("flink"))
//    flinkTweets.print()

//    stream2.flatMap(new MyFlatMap).print()

    //侧输出流
    //    val monitoredReadings: DataStream[SensorReading] = stream1
    //      .process(new FreezingMonitor)
    //    monitoredReadings
    //      .getSideOutput(new OutputTag[String]("freezing-alarms"))
    //      .print()
    //    monitoredReadings
    //      .getSideOutput(new OutputTag[String]("freezing-alarms2"))
    //      .print()
    env.execute()
  }

  class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading] {

    import org.apache.flink.streaming.api.scala._

    // 定义一个侧输出标签
    lazy val freezingAlarmOutput: OutputTag[String] = new OutputTag[String]("freezing-alarms")
    lazy val freezingAlarmOutput2: OutputTag[String] = new OutputTag[String]("freezing-alarms2")

    override def processElement(r: SensorReading,
                                ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                                out: Collector[SensorReading]): Unit = {
      // 温度在 32F 以下时，输出警告信息
      if (r.temperature < 32.0) {
        ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${r.id}")
      } else {
        ctx.output(freezingAlarmOutput2, s"sssss ${r.id}")
      }
      // 所有数据直接常规输出到主流
      out.collect(r)
    }

  }

  //udf的富函数
  class MyFlatMap extends RichFlatMapFunction[Int, (Int, Int)] {
    var subTaskIndex = 0
    override def open(configuration: Configuration): Unit = {
      subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
      val cof = getRuntimeContext.getExecutionConfig
      // 以下可以做一些初始化工作，例如建立一个和 HDFS 的连接
    }
    override def flatMap(in: Int, out: Collector[(Int, Int)]): Unit = {
      if (in % 2 == subTaskIndex) {
        out.collect((subTaskIndex, in))
      } }
    override def close(): Unit = {

    }
  }
}
