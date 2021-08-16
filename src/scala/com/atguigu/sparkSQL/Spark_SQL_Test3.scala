package com.atguigu.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author smh
 * @create 2021-06-06 16:40
 */
object Spark_SQL_Test3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("D:\\workspace\\Spark-SQL\\input\\user.json")
    //df的sql语法
    /*
    创建视图 四种方式
    两种局部,两种全局
    全局视图调用的时候要加上 global_temp.视图名,局部视图只能在当前会话调用
    局部和全局各有两种方式,创建,创建或替代,如果视图已经存在用第一种方式创建会报错
     */
    df.createTempView("user")
    //sql语法格式
//    spark.sql("select avg(age) from user")
    //DSL语法格式
//    df.select("name","age").where("name").show()
    spark.sql("select avg(age) from user where name=\"xuzhu\"" ).show
  }
}
