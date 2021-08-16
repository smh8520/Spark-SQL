package com.atguigu.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author smh
 * @create 2021-06-07 9:53
 */
object Spark_Load_Data {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test1").setMaster("local[*]")
    conf.set("spark.sql.sources.default","json")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //加载数据 read  可以指定加载模式
    //read.json  读取json文件
    spark.read.json("D:\\workspace\\Spark-SQL\\input\\user.json").show()
    //csv 读取以逗号为分隔符的文件 这种方式读取没有列名
    spark.read.csv("D:\\workspace\\Spark-SQL\\input\\user.txt").show()


    //默认的加载模式 load() 默认是加载parquet模式的文件,可以使用format指定加载的文件格式
    //也可以在配置文件中修改默认的加载格式
    spark.read.load("D:\\workspace\\Spark-SQL\\input\\user.json").show()
  }
}
