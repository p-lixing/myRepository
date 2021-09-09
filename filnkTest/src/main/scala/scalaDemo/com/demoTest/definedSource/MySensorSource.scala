package scalaDemo.com.demoTest.definedSource


import org.apache.flink.streaming.api.functions.source.SourceFunction
import scalaDemo.com.demoTest.waterMarkUtils.Sensor.SensorReading

import scala.util.Random

class MySensorSource extends SourceFunction[SensorReading] {
  // flag: 表示数据源是否还在正常运行
  var running: Boolean = true

  override def cancel(): Unit = {
    running = false
  }

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit
  = { // 初始化一个随机数发生器
    val rand = new Random()
    var curTemp = 1.to(10).map(i => ("sensor_" + i, 65 + rand.nextGaussian() * 20))
    while (running) {
      // 更新温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + rand.nextGaussian())
      )
      // 获取当前时间戳
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        t => ctx.collect(SensorReading(t._1, curTime, t._2))
      )
      Thread.sleep(100)
    }
  }
}
