package com.baway.flink.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._
import org.apache.flink.table.types.DataType

/**
  * @ author yulong
  * @ createTime 2020-05-02 7:17
  */
object TableApiTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //创建表环境
    //1.1 创建老版本的流查询环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useAnyPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 1.2  创建老版本的批式查询环境
    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)

    // 1.3 创建blink版本的流查询环境
    //    val bsSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
    //      .useBlinkPlanner()
    //      .inStreamingMode()
    //      .build()
    //    val bsTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bsSettings)

    // 1.4 创建blink版本的批式查询环境
    //    val bbSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
    //      .useBlinkPlanner()
    //      .inBatchMode()
    //      .build()
    //    val bbTableEnv: TableEnvironment = TableEnvironment.create(bbSettings)

    //  从外部系统读取数据，在环境中注册表
    val filePath = "D:\\Mywork\\workspace\\IdeaProjects\\baway2019\\flink-table1100\\src\\main\\resources\\sensor.txt"
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv()) //定义读取数据之后的csv方法
      .withSchema(new Schema()
      .field("id", DataTypes.STRING())
      .field("timestemp", DataTypes.BIGINT())
      .field("temperature", DataTypes.DOUBLE())
    ) //定义表结构
      .createTemporaryTable("inputTable")

    //  2.2 连接到Kafka
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sensor")
      .property("bootstrap.servers", "hadoop102:9092")
      .property("zookeeper.connect", "hadoop102:2181")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestemp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      ).createTemporaryTable("kafkaInputTable")

    //  3. 表的查询
    //  3.1 简单查询，过滤投影
    val sensorTable: Table = tableEnv.from("inputTable")
    val resultTable: Table = sensorTable
      .select('id, 'temperature) //  单引号+字段名，表示表里面的一个字段
      .filter('id === "sensor_1") //  字段和字符串进行比较使用3个等号

    //  3.2 SQL简单查询
    val sqlResultTable: Table = tableEnv.sqlQuery(
      """
        select id, temperature
        from inputTable
        where id = 'sensor_1'
      """.stripMargin)

    //  3.3 简单聚合，统计每个传感器温度个数
    val aggResultTable: Table = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count)
    //  3.4 SQL实现简单聚合
    val aggResultSqlTable: Table = tableEnv.sqlQuery("select id, count(id) as cnt from inputTable group by id")


    //转换成流打印输出
    //    val sensorTable: Table = tableEnv.from("kafkaInputTable")
//    sqlResultTable.toAppendStream[(String, Double)].print("resultTable")
//    aggResultTable.toRetractStream[(String, Long)].print("agg")
    aggResultSqlTable.toRetractStream[(String, Long)].print("agg")

    env.execute("table api test job")


  }

}
