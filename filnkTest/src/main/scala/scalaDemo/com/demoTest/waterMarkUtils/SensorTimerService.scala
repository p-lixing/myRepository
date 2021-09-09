package scalaDemo.com.demoTest.waterMarkUtils

import org.apache.flink.api.common.functions.{RichFilterFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import scalaDemo.com.demoTest.definedSource.MySensorSource

/**
  *
  * 监控温度传感器的温度值，如果温度值在一秒钟之内(processing time)连
  * 续上升，则报警。
  *
  **/

object SensorTimerService {

  // 定义样例类，传感器 id，时间戳，温度
  case class SensorReading(id: String, timestamp: Long, temperature: Double)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream4 = env.addSource(new MySensorSource())
    val stream1 = env
      .fromCollection(List(
        SensorReading("sensor_1", 1627613049, 35.80018327300259),
        SensorReading("sensor_6", 1627613075, 15.402984393403084),
        SensorReading("sensor_6", 1627613075, 18.402984393403084),
        SensorReading("sensor_6", 1627613075, 19.402984393403084),
        SensorReading("sensor_7", 1627613082, 6.720945201171228),
        SensorReading("sensor_10", 1627613093, 38.101067604893444)
      ))
      // event time 的引入
      .assignAscendingTimestamps(t => t.timestamp)
    val warnings = stream1
      .keyBy(_.id)
      .process(new TempIncreaseAlertFunction)
    //      .print()

    //键控状态
    val alerts: DataStream[(String, Double, Double)] = stream1.keyBy(_.id)
      .flatMapWithState[(String, Double, Double), Double] {
      case (in: SensorReading, None) =>
        (List.empty, Some(in.temperature))
      case (r: SensorReading, lastTemp: Some[Double]) =>
        val tempDiff = (r.temperature - lastTemp.get).abs
        if (tempDiff > 1.7) {
          (List((r.id, r.temperature, tempDiff)), Some(r.temperature))
        } else {
          (List.empty, Some(r.temperature))
        }
    }
    alerts.print()


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

  class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, String] {
    // 保存上一个传感器温度值
    lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("lastTemp", Types.of[Double])
    )
    // 保存注册的定时器的时间戳
    lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )

    override def processElement(r: SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                                out: Collector[String]): Unit = {
      // 取出上一次的温度
      val prevTemp = lastTemp.value()
      //      println("tmp:::"+prevTemp)
      // 将当前温度更新到上一次的温度这个变量中
      lastTemp.update(r.temperature)
      val curTimerTimestamp = currentTimer.value()
      //      println("cur:::"+curTimerTimestamp)
      if (prevTemp == 0.0 || r.temperature < prevTemp) {
        // 温度下降或者是第一个温度值，删除定时器
        //        ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
        ctx.timerService().deleteEventTimeTimer(curTimerTimestamp)
        // 清空状态变量
        currentTimer.clear()
      } else if (r.temperature > prevTemp && curTimerTimestamp == 0) {
//        println("=sdad==" + r.temperature + "======:" + prevTemp + "==========:" + curTimerTimestamp)
        // 温度上升且我们并没有设置定时器
        //        val timerTs = ctx.timerService().currentProcessingTime() + 1000
        val timerTs = ctx.timerService().currentWatermark() + 1000
//        println("=====timerTs:" + timerTs)
        //        ctx.timerService().registerProcessingTimeTimer(timerTs)
        ctx.timerService().registerEventTimeTimer(timerTs)
        currentTimer.update(timerTs)
      }
    }

    override def onTimer(ts: Long,
                         ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
//      println("=============:" + ctx.getCurrentKey + "==========:" + currentTimer.value())
      out.collect("传感器 id 为: " + ctx.getCurrentKey + "的传感器温度值已经连续 1s 上升了。")
      currentTimer.clear()
    }
  }

  class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading] {

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

}
