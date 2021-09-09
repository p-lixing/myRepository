package scalaDemo.com.demoTest.waterMarkUtils

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

object wordcount {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 从文件中读取数据
    val inputPath = "D:\\ideaWorkspace\\filnkTest\\src\\main\\resources\\test_data.txt"
    val inputDS: DataSet[String] = env.readTextFile(inputPath)
    import org.apache.flink.api.scala._
    // 分词之后，对单词进行 groupby 分组，然后用 sum 进行聚合
    val wordCountDS: AggregateDataSet[(String, Int)] = inputDS.flatMap(x =>x.split(" ")).map((_,1)).groupBy(0).sum(1)
    // 打印输出
    wordCountDS.print()
  }
}
