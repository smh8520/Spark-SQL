package com.atguigu.sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
 * @author smh
 * @create 2021-06-07 16:46
 */
object Spark_Hive_Click_TopN {
  def main(args: Array[String]): Unit = {
    System.setProperty("Hadoop_USER_NAME","atguigu")
    val conf: SparkConf = new SparkConf().setAppName("click_topN").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    spark.udf.register("click_remark",functions.udaf(new Click_Remark))

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
        |""".stripMargin).show(1000,false)

    spark.stop()
  }
}

case class Buff(var totalCount:Long,var cityCount:mutable.Map[String,Long])

class Click_Remark extends Aggregator[String,Buff,String] {
  override def zero: Buff = Buff(0L,mutable.Map[String,Long]())

  override def reduce(b: Buff, a: String): Buff = {
    b.totalCount+=1
    b.cityCount(a)=b.cityCount.getOrElse(a,0L)+1L
    b
  }

  override def merge(b1: Buff, b2: Buff): Buff ={
    b1.totalCount+=b2.totalCount
    b2.cityCount.foreach{
      case (k,v)=> {
        b1.cityCount(k) = b1.cityCount.getOrElse(k, 0L) + v
      }
    }
    b1
  }

  override def finish(reduction: Buff): String = {
    var sortList: List[(String, Long)] = reduction.cityCount.toList.sortWith(_._2>_._2).take(2)
    var buff = ListBuffer[String]()
    var sum = 0L
    sortList.foreach{
      case (city,count)=>{
        val l: Long = count*100/reduction.totalCount
        sum+=l
        buff.append(city+" "+l+"%")
      }
    }
    if(reduction.cityCount.size>2){
      buff.append("其它 "+(100-sum)+"%")
    }
    buff.mkString(",")
  }

  override def bufferEncoder: Encoder[Buff] =Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}