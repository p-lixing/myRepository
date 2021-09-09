package scalaDemo.com.demoTest.waterMarkUtils

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.mutable


/**
  * nc -lk 9999
  *
  * /usr/local/flink/bin/flink run -m yarn-cluster -ys 1 -ynm flinkTest -yn 1 -yjm 1024 -ytm 1024 -d -c com.demoTest.waterMarkUtils.eventimeWindow filnkTest-1.0-SNAPSHOT.jar
  *
  * -m 运行模式，这里使用yarn-cluster，即yarn集群模式。
  * -ys slot个数。
  * -ynm Yarn application的名字。
  * -yn task manager 数量。
  * -yjm job manager 的堆内存大小。
  * -ytm task manager 的堆内存大小。
  * -d detach模式。可以运行任务后无需再控制台保持连接。
  * -c 指定jar包中class全名
  *
  * */

object eventimeWindow {
  def main(args: Array[String]): Unit = {
    // 环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    val dstream: DataStream[String] = env.socketTextStream("172.16.122.48", 9999)
    val textWithTsDstream: DataStream[(String, Long, Int)] = dstream.map { text =>
      val arr: Array[String] = text.split(",")
      (arr(0), arr(1).toLong, 1)
    }
    val textWithEventTimeDstream: DataStream[(String, Long, Int)] =
      textWithTsDstream.assignTimestampsAndWatermarks(new
          BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.milliseconds(1000)) {
        override def extractTimestamp(element: (String, Long, Int)): Long = {
          return element._2
        }
      })
    val textKeyStream: KeyedStream[(String, Long, Int), Tuple] = textWithEventTimeDstream.keyBy(0)
    //    textKeyStream.print("textkey:")
    //滚动窗口
    val windowStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow] = textKeyStream.window(TumblingEventTimeWindows.of(Time.seconds(2)))
    //滑动窗口
    //    val windowStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow] = textKeyStream.window(SlidingEventTimeWindows.of(Time.seconds(2), Time.milliseconds(500)))
    //session会话窗口
//    val windowStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow]= textKeyStream.window(EventTimeSessionWindows.withGap(Time.milliseconds(500)))

    val groupDstream: DataStream[mutable.HashSet[Long]] =
      windowStream.fold(new mutable.HashSet[Long]()) { case (set, (key, ts, count))
      =>
        set += ts
      }
    groupDstream.print("window::::").setParallelism(1)
    env.execute()
  }
}
