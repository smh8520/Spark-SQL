package com.atguigu.sparkSQL

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author smh
 * @create 2021-06-27 23:38
 */
object test1 {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)

    val value: Broadcast[String] = sc.broadcast("a")

    //关闭sc
    sc.stop()
  }
}

class s extends AccumulatorV2[String,Long] {
  override def isZero: Boolean = ???

  override def copy(): AccumulatorV2[String, Long] = ???

  override def reset(): Unit = ???

  override def add(v: String): Unit = ???

  override def merge(other: AccumulatorV2[String, Long]): Unit = ???

  override def value: Long = ???
}
