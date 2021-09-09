package scalaDemo.com.flinkSource.hbaseSorceUtils.RichSourceFunctionDemo

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment,_}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}

object hbaseSourceMain {

  def main(args: Array[String]): Unit = {
    readFromHBaseWithRichSourceFunction()

  }
  /**
    * 从HBase读取数据
    * 第一种：继承RichSourceFunction重写父类方法
    */
  def readFromHBaseWithRichSourceFunction(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val dataStream: DataStream[(String, String)] = env.addSource(new HBaseReader)
    dataStream.map(x => println(x._1 + " " + x._2))
    env.execute()
  }
}
