package scalaDemo.com.demoTest.cepDemo

import java.util

import org.apache.flink.cep.{PatternFlatSelectFunction, PatternSelectFunction}
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

case class Warning1(userId: String, eventTime: String, eventTime2: String, warningMsg: String)

case class timeoutEvent(userId: String, eventTime: String, eventTime2: String, warningMsg: String)

case class LoginEvent(userId: String, ip: String, eventType: String, eventTime: String)

object cepTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val loginEventStream = env.fromCollection(List(
      LoginEvent("1", "192.168.0.1", "fail", "1627954317"),
      LoginEvent("1", "192.168.0.2", "fail", "1627954367"),
      LoginEvent("1", "192.168.0.3", "fail", "1627954377"),
      LoginEvent("2", "192.168.10.10", "success", "1627954387")
    )).assignAscendingTimestamps(_.eventTime.toLong)

    val patt = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType.equals("fail"))
      .next("next")
      .where(_.eventType.equals("fail"))
      .within(Time.minutes(10))

    //    val loginFailPattern = Pattern.begin[LoginEvent]("start")
    //      .where(new SimpleCondition[LoginEvent]{
    //        override def filter(t: LoginEvent): Boolean = {
    //          t.eventType.equals("fail")
    //        }
    //      })
    //      .next("next")
    //      .where(new SimpleCondition[LoginEvent]{
    //        override def filter(t: LoginEvent): Boolean = {
    //          t.eventType.equals("fail")
    //        }
    //      })
    //      .within(Time.seconds(10))


    val patternStream = CEP.pattern(loginEventStream.keyBy(_.userId), patt)
    //flatSelect和select区别， flatSelect 方法可以返回多条记录  Select 方法可以返回1条记录
    //    patternStream.select(new EventMatch2()).print()
    patternStream.flatSelect(new EventMatche()).print()




    //    val loginFailDataStream = patternStream
    //      .select((pattern: Map[String, Iterable[LoginEvent]]) => {
    //        val first = pattern.getOrElse("begin", null).iterator.next()
    //        val second = pattern.getOrElse("next", null).iterator.next()
    //        Warning1(first.userId, first.eventTime, second.eventTime, "warning")
    //      })
    env.execute("cc")
  }

  // 重写PatternSelectFunction方法，用Warning样例类来接收数据
  class EventMatch2() extends PatternSelectFunction[LoginEvent, Warning1] {
    override def select(map: util.Map[String, util.List[LoginEvent]]): Warning1 = {
      val first = map.getOrDefault("begin", null).get(0)
      val second = map.getOrDefault("next", null).get(0)
      Warning1(first.userId, first.eventTime, second.eventTime, "warning")
    }
  }

  // 重写PatternSelectFunction方法，用Warning样例类来接收数据
  class EventMatche extends PatternFlatSelectFunction[LoginEvent, Warning1] {
    override def flatSelect(map: util.Map[String, util.List[LoginEvent]], collector: Collector[Warning1]): Unit = {
      val first = map.getOrDefault("begin", null).get(0)
      val second = map.getOrDefault("next", null).get(0)
      collector.collect(Warning1(first.userId, first.eventTime, second.eventTime, "warning"))
      collector.collect(Warning1(first.userId, first.eventTime, second.eventTime, "timeout"))

    }
  }


}
