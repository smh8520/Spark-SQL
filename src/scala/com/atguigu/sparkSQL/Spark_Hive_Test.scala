package com.atguigu.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author smh
 * @create 2021-06-07 11:44
 */
object Spark_Hive_Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val conf: SparkConf = new SparkConf().setAppName("hiveTest").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    spark.sql("show tables").show()
//    spark.sql("create table student(name String)")
    spark.sql("drop table student")
//
    spark.sql("show tables").show()

    spark.stop()
  }
}
