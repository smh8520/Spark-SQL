package com.atguigu.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author smh
 * @create 2021-06-05 14:44
 */
object SparkSQL_MyUDAF {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkTestSQL").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


//    import spark.implicits._
    val df: DataFrame = spark.read.json("D:\\workspace\\Spark-SQL\\input\\user.json")
//    val rdd: RDD[Row] = df.rdd
//    rdd.collect().foreach(println)
//
//    val userRDD: RDD[User] = rdd.map(x=>User(x.getString(1),x.getLong(0)))
//
//    userRDD.toDF().show()
    df.createTempView("User")

    spark.udf.register("myName",(name:String)=>{"Name:"+name})

    spark.sql("select myName(name) from user").show()

  }
}
case class User(name:String,age:Long)