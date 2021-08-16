package com.atguigu.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author smh
 * @create 2021-06-07 10:01
 */
object Spark_Save_Data {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test2").setMaster("local[*]")
//    conf.set("spark.sql.sources.default","json")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("D:\\workspace\\Spark-SQL\\input\\user.json")

    //保存数据  df.write.格式("路径")

    df.write.csv("D:\\workspace\\Spark-SQL\\output")
    //默认的保存 .save是以parquent的格式保存的,可以使用format指定保存格式,也可以在配置文件中修改默认文件格式
    df.write.mode("overwrite").save("D:\\workspace\\Spark-SQL\\output1")
    /*
    write.mode() 指定保存模式
    四种保存模式
    append 文件夹存在则追加
    ignore 文件夹存在则忽略此次操作
    overwrite 文件夹存在则覆盖
    errorIfExists 存在则报错,是默认的模式
     */
  }
}
