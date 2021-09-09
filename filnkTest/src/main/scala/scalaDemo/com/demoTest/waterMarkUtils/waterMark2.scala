package scalaDemo.com.demoTest.waterMarkUtils

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object waterMark2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置event_time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //waterMark 引入间隔 90秒
    env.getConfig.setAutoWatermarkInterval(90000)
    import org.apache.flink.streaming.api.scala._
    val input=env.fromCollection(List(("b",1L),("b",2L),("b",3L),("b",4L),("b",5L),("b",6L),("b",7L),("b",6L),("c",8L),("b",9L)))
    //waterMark 引入 每隔 90 秒产生一个 watermark
    val timeWindow=input.assignTimestampsAndWatermarks(
      //代表最长的时延1ms Time.milliseconds(1)
      new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.milliseconds(1)) {
        // 抽取时间戳
        override def extractTimestamp(element: (String, Long)): Long = element._2
      })

    val result=timeWindow.keyBy(0).timeWindow(Time.milliseconds(4)).sum("_2")
//    val tt = input.assignTimestampsAndWatermarks(new PunctuatedAssigner)
//    val result=tt.keyBy(0).timeWindow(Time.milliseconds(4)).sum("_2")
    result.print()

    env.execute()
  }

  //间断式地生成 watermark
  class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[(String, Long)] {
    val bound: Long = 60 * 1000
    override def checkAndGetNextWatermark(t: (String, Long), extractedTS: Long): Watermark = {
      if (t._1 == "c") {
        new Watermark(extractedTS - bound) } else {
        null
      }
    }
    override def extractTimestamp(t: (String, Long), l: Long): Long = {
      t._2
    }
  }

}
