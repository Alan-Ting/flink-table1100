package com.baway.flink.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

/**
  * @ author yulong
  * @ createTime 2020-05-01 22:42
  */
object TableExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //读取数据创建DataStream
    //基于文件获取数据
//    val inputStream = env.readTextFile("D:\\Mywork\\workspace\\IdeaProjects\\baway2019\\flink-table1100\\src\\main\\resources\\sensor.txt")

    //基于端口获取数据
    val inputStream: DataStream[String] = env.socketTextStream("hadoop102", 7777)

    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong, dataArr(2).trim.toDouble)
    })

    //创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //基于数据流，转换成一张表
    val dataTable: Table = tableEnv.fromDataStream(dataStream)

    //调用table API，得到转换结果
//    val resultTable: Table = dataTable
//      .select("id, temperature")
//      .filter("id == 'sensor_1'")

    //使用sql方式，得到转换结果
    val resultTable: Table = tableEnv.sqlQuery("select id, temperature from " + dataTable + " where id = 'sensor_1'")

    //转换回数据流打印输出
    val resultStream: DataStream[(String, Double)] = resultTable.toAppendStream[(String, Double)]

    resultStream.print("result")

    env.execute("table example job")


  }

}

// 温度传感器读数样例类
case class SensorReading(id:String,timestamp: Long, temperature:Double)
