package com.baway.flink.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.table.types.DataType

/**
  * @ author yulong
  * @ createTime 2020-05-04 9:41
  */
object outPutTableTest {

  def main(args: Array[String]): Unit = {
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //读取数据创建dataStream
    val inputStream: DataStream[String] = env.readTextFile("D:\\Mywork\\workspace\\IdeaProjects\\baway2019\\flink-table1100\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArr: Array[String] = data.split(",")
        SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
      })

    //创建表环境——老版本流查询环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //将DataStream转换成Table
    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature as 'temp, 'timestamp as 'ts)
    //对Table进行转换操作，得到结果表
    val resultTable: Table = sensorTable.select('id, 'temp)
      .filter('id === "sensor_1")

    val aggResultTable: Table = sensorTable.groupBy('id)
      .select('id, 'id.count as 'count)


    //定义一张输出表,就是要写入数据的TableSink
    tableEnv.connect( new FileSystem().path("D:\\Mywork\\workspace\\IdeaProjects\\baway2019\\flink-table1100\\output\\out.txt") )
        .withFormat( new Csv() )
        .withSchema( new Schema()
            .field("id", DataTypes.STRING())
            .field("temp", DataTypes.DOUBLE() )
        ).createTemporaryTable("outputTable")

    //将结果写入table sink
    resultTable.insertInto("outputTable")

//    sensorTable.printSchema()
//    sensorTable.toAppendStream[(String, Double, Long)].print()
    env.execute("output table test")

  }

}
