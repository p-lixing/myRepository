package scalaDemo.com.demoTest.streamToTble

import java.sql.Timestamp

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object FlinkSqlReadFileTable {
  def main(args: Array[String]): Unit = {
    // 构建流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 构建table运行环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    // 使用流处理来读取数据
    val readData = env.readTextFile("D:\\ideaWorkspace\\filnkTest\\src\\main\\resources\\test_data.txt")
    // 使用flatMap进行切分
    val word: DataStream[String] = readData.flatMap(_.split(" "))
    // 将word 转为 table 'tt.proctime为时间字段
    val table = tableEnv.fromDataStream(word, 'id, 'tt.proctime)

    //sql方式
    //    tableEnv.registerTable("test",table)
        val re = tableEnv.sqlQuery("select id,tt from test")
//    val re = tableEnv.sqlQuery("select id,tt from " + table)

    // 计算wordcount
    //    val wordCount = table.groupBy("f0").select('f0, 'f0.count as 'count)
    val wordCount = table.groupBy("id").select('id, 'id.count as 'count)
    //    wordCount.printSchema()
    //转换成流处理打印输出toAppendStream和toRetractStream
    //如果数据只是不断添加，可以使用追加模式，其余方式则不可以使用追加模式，而缩进模式侧可以适用于更新，删除等场景
    //    tableEnv.toRetractStream[(String,Long)](wordCount).print()
    val res: DataStream[(Boolean, (String, Long))] = wordCount.toRetractStream[(String, Long)]
    //    res.print()
    tableEnv.toRetractStream[(String, Timestamp)](re).print()

    env.execute("FlinkSqlReadFileTable")
  }
}
