package scalaDemo.com.demoTest.waterMarkUtils

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object waterMarkTest {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val input=env.fromCollection(List(("a",1L),("b",1L),("b",5L),("b",5L)))
    val timeWindow=input.assignAscendingTimestamps(t=>t._2)
    val result=timeWindow.keyBy(0).timeWindow(Time.milliseconds(4)).sum("_2")
    result.print()
    env.execute()
  }
}
