package com.atguigu.sparkSQL


import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedFunction}

/**
 * @author smh
 * @create 2021-06-06 17:10
 */
object Test {
  def main(args: Array[String]): Unit = {
    //创建Spark配置文件
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    spark.udf.register("myName",(name:String)=>"::"+name)

    val df: DataFrame = spark.read.json("D:\\workspace\\Spark-SQL\\input\\user.json")
    //df的sql语法
    /*
    创建视图 四种方式
    两种局部,两种全局
    全局视图调用的时候要加上 global_temp.视图名,局部视图只能在当前会话调用
    局部和全局各有两种方式,创建,创建或替代,如果视图已经存在用第一种方式创建会报错
     */
    df.createTempView("user")
//    spark.udf.register("myAvg",functions.udaf(new MyAvg))
////    spark.udf.register("mySum",functions.udaf(new mySum))
//    spark.sql("select myAvg(age) as myName from user").show()
//    spark.sql("select mySum(age) as sum from user").show()
    df.select((df.col("age")+1).name("age")).show()

  }
}
case class Buf(var num:Long,var count:Long)
class MyAvg extends Aggregator[Long,Buf,Double] {
  override def zero: Buf = Buf(0L,0L)

  override def reduce(b: Buf, a: Long): Buf = {
    b.num+=a
    b.count+=1
    b
  }

  override def merge(b1: Buf, b2: Buf): Buf = {
    b1.count+=b2.count
    b1.num+=b2.num
    b1
  }

  override def finish(reduction: Buf): Double = reduction.num.toLong/reduction.count

  override def bufferEncoder: Encoder[Buf] =Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

case class Buff(var sum:Long,var count:Long)

class AvgS extends Aggregator[Long,Buff,Double]{
  override def zero: Buff = Buff(0L,0L)

  override def reduce(b: Buff, a: Long): Buff = {
    b.sum+=a
    b
  }

  override def merge(b1: Buff, b2: Buff): Buff = {
    b1.sum+=b2.sum
    b1.count+=b2.count
    b1
  }

  override def finish(reduction: Buff): Double = reduction.sum.toDouble/reduction.count

  override def bufferEncoder: Encoder[Buff] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}