package scalaDemo.com.demoTest.streamToTble

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object readCsvTable {
  def main(args: Array[String]): Unit = {
    //用于递归读取文件配置参数，默认是False
//    val parameters = new Configuration
//    parameters.setBoolean("recursive.file.enumeration", true)

    // 构建流处理运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 构建table运行环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    // 使用流处理来读取数据
    val readData = env.readCsvFile[String]("C:\\Users\\86184\\Desktop\\test_data.txt", includedFields = Array(0))
    // 使用递归方式读取数据
    //      .withParameters(parameters).print()
    val table = tableEnv.fromDataSet(readData)

    val wordCount = table.groupBy("name").select('name, 'name.count as 'count)
    //    wordCount.printSchema()
    //转换成流处理打印输出
    env.execute("FlinkSqlReadFileTable")

  }
}
