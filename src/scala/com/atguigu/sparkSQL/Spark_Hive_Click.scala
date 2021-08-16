package com.atguigu.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author smh
 * @create 2021-06-17 11:14
 */
object Spark_Hive_Click {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("t").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    spark.udf.register("click_remark",functions.udaf(new MyUDAF))

    spark.sql(
      """
        |select area,product_name,click_count,city_click
        |from(
        |select area,product_name,click_count, city_click,rank()over(partition by area order by click_count desc) rk
        |from(
        |select area,product_name,count(*) click_count,click_remark(city_name) city_click
        |from(
        |select c.area,c.city_name,p.product_name
        |from
        |(
        |select * from user_visit_action where click_product_id!=-1
        |) t
        |join city_info c
        |on t.city_id = c.city_id
        |join product_info p
        |on t.click_product_id = p.product_id
        |)t1
        |group by area,product_name
        |)t2)t3
        |where rk<=3
        |""".stripMargin).show()

    spark.stop()
  }
}

case class Buffer(var total:Long,map:mutable.Map[String,Long])

class MyUDAF extends Aggregator[String,Buffer,String] {

  override def zero: Buffer =Buffer(0L,mutable.Map[String,Long]())

  override def reduce(b: Buffer, a: String): Buffer = {
    b.total+=1L
    b.map(a)=b.map.getOrElse(a,0L)+1L
    b
  }

  override def merge(b1: Buffer, b2: Buffer): Buffer = {
    b1.total+=b2.total
    b2.map.foreach{
      case (key,value)=>{
        b1.map(key)=b1.map.getOrElse(key,0L)+value
      }
    }
    b1
  }

  override def finish(reduction: Buffer): String = {
    var list: List[(String, Long)] = reduction.map.toList.sortWith(_._2>_._2).take(2)
    var listBuffer: ListBuffer[String] = ListBuffer[String]()
    var sum = 0L
    list.foreach{
      case (city,cnt)=>{
        val l: Long = cnt*100/reduction.total
        sum+=l
        listBuffer.append(city+" "+l+"%")
      }
    }
    if(reduction.map.size>2){
      listBuffer.append("其它 "+(100-sum)+"%")
    }
    listBuffer.mkString(",")
  }

  override def bufferEncoder: Encoder[Buffer] = Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}