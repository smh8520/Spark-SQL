package com.atguigu.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @author smh
 * @create 2021-06-06 16:32
 */
object Spark_SQL_Test2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("D:\\workspace\\Spark-SQL\\input\\user.json")

    import spark.implicits._
    //df->ds as[类型] 会自动将df的字段与ds类型的属性进行匹配,顺序没关系,但是名字对不上的话就会报错
    //ds->df 默认是按照类型的属性转换成字段名,也可以指定.
    val ds: Dataset[User] = df.as[User]
    ds.show()

    ds.toDF().show()
  }

}
