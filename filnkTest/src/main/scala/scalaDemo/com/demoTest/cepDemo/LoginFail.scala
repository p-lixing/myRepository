package scalaDemo.com.demoTest.cepDemo

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector
import scala.collection.mutable.ListBuffer




object LoginFail {
  case class LoginEvent1(userId: Long, ip: String, eventType: String, eventTime: Long)
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginEventStream = env.fromCollection(List(
      LoginEvent1(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent1(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent1(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent1(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .keyBy(_.userId)
      .process(new MatchFunction())
      .print()

    env.execute("Login Fail Detect Job")
  }

  class MatchFunction extends KeyedProcessFunction[Long, LoginEvent1, LoginEvent1] {

    // 定义状态变量
    lazy val loginState: ListState[LoginEvent1] = getRuntimeContext.getListState(
      new ListStateDescriptor[LoginEvent1]("saved login", classOf[LoginEvent1]))

    override def processElement(login: LoginEvent1,
                                context: KeyedProcessFunction[Long, LoginEvent1,
                                  LoginEvent1]#Context, out: Collector[LoginEvent1]) = {

      if (login.eventType == "fail") {
        loginState.add(login)
      }
      // 注册定时器，触发事件设定为2秒后
      context.timerService.registerEventTimeTimer(login.eventTime.toLong + 2 * 1000)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, LoginEvent1,
                           LoginEvent1]#OnTimerContext, out: Collector[LoginEvent1]): Unit = {


      val allLogins: ListBuffer[LoginEvent1] = ListBuffer()
      import scala.collection.JavaConversions._
      for (login <- loginState.get) {
        allLogins += login
      }
      loginState.clear()

      if (allLogins.length > 1) {
        out.collect(allLogins.head)
      }
    }
  }

}
