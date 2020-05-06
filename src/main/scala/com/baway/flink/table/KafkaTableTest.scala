package com.baway.flink.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
  * @ author yulong
  * @ createTime 2020-05-06 16:05
  */
object KafkaTableTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //创建老版本的流查询环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //Kafka进 Kafka出
    //连接到Kafka
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sensor")
      .property("bootstrap.servers", "hadoop102:9092")
      .property("zookeeper.connect", "hadoop102:2181")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestramp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())

      )
      .createTemporaryTable("kafkaInputTable")

    //做转换操作
    // 对Table进行转换，得到结果表
    val sensorTable: Table = tableEnv.from("kafkaInputTable")
    val resultTable: Table = sensorTable.select('id, 'temp)
      .filter('id === "sensor_1")

  }

}
