package com.atguigu.sparkSQL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author smh
 * @create 2021-06-06 16:11
 */
object Spark_SQL_Test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkSqlTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val userRDD: RDD[String] = sc.textFile("D:\\workspace\\Spark-SQL\\input\\user.txt")


    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    //rdd与DataFrame之间的转换
    //rdd->df
    //相互转换必须要导入SparkSession对象下所有的隐式转换
    //rdd只关心数据本身,不会关心数据结构
    //rdd->df 使用toDF方法,在方法中可以指定字段名
    //如果rdd读入的是一行数据,要切分成元组,df会按照元组的元素来确定字段
    //也可以将rdd的数据映射成一个样例类,这样不用指定列名,会自动按照属性名来确定字段名
    val df: DataFrame = userRDD.map(x=>{
      val strings: Array[String] = x.split(",")
      User(strings(0),strings(1).toLong)
    }).toDF()
    df.show()

    //df->rdd  调用.rdd方法,转换出来的rdd是row类型
    //因为df本身就是一个存储row类型的,是一个特殊的dataSet
    val rdd: RDD[Row] = df.rdd
    rdd.collect().foreach(println)
  }
}
