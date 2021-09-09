package scalaDemo.com.demoTest.cepDemo

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._
import java.util

// 定义样例类，Event用来存放输入数据，Warning存放输出数据
case class Event(deviceId: String, tempBefore: String, tempNow: String, timestamp: String)
case class Warning(deviceId: String, tempNow: String, warningMsg: String)

object StreamCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 使用readFile方法，流式的从文本文件里读取数据，当source.txt里的数据有新增时会检测到并读入
    val reader = new TextInputFormat(null)
//    val inputStream = env.readTextFile("src/main/resources/source.txt")
    val inputStream = env.readFile(reader, "src/main/resources/source.txt", FileProcessingMode.PROCESS_CONTINUOUSLY, 10)
    val eventStream = inputStream
      .map(data => {
        val arr = data.split(",")
        Event(arr(0), arr(1), arr(2), arr(3))
      }).assignAscendingTimestamps(_.timestamp.toLong)

    // 定义Pattern规则，匹配当前温度大于上次的温度超过6次的数据
    val eventPattern = Pattern.begin[Event]("start")
      .where(data => data.tempNow > data.tempBefore).times(6)

    val patternStream = CEP.pattern(eventStream, eventPattern)

    // 使用select方法来捕捉结果，EventMatch为捕捉结果的函数
    val resultDataStream = patternStream.select(new EventMatch)

    // 导出数据为文本文件，但使用StreamingFileSink方法，结果并不会输出为一个sink.txt文件，而是导出为分布式文件结构，会自动生成目录和文件
    val sink = StreamingFileSink
      .forRowFormat(new Path("src/main/resources/sink.txt"), new SimpleStringEncoder[Warning]("UTF-8"))
      .build()
    resultDataStream.addSink(sink)

    // 或者直接打印出信息
    resultDataStream.print()

    env.execute("temperature detect job")
  }
}

// 重写PatternSelectFunction方法，用Warning样例类来接收数据
class EventMatch() extends PatternSelectFunction[Event, Warning] {
  override def select(pattern: util.Map[String, util.List[Event]]): Warning = {
    val status = pattern.get("start").get(0)
    Warning(status.deviceId, status.tempNow, "Temperature High Warning")
  }
}
