package com.atguigu.sparkSQL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-06-06 16:24
 */
object Spark_SQL_Test1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("D:\\workspace\\Spark-SQL\\input\\user.txt")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    //DataSet是强数据类型,转换为ds时必须要指定数据的类型
    //ds的每一行数据就相当于是一个类的一个对象
    //df是ds的特殊类型,row
    //rdd和df转换为ds时一般都要将数据转化为样例类
    val ds: Dataset[User] = rdd.map(
      x => {
      val strings: Array[String] = x.split(",")
      User(strings(0), strings(1).toLong)
    }).toDS()

    ds.show()
    //ds转化为rdd时就会转化成对应样例类的rdd
    val rdd1: RDD[User] = ds.rdd
  }
}
