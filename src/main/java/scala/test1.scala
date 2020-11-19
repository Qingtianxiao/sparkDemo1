package scala

import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by ligc on 2020/11/16 18:45
 */
class test1 {

}

object test1{
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local")
      .appName("demo3")
      .getOrCreate()
    val ret:Array[Row] = spark.range(2).toDF().collect()
    for(row <- ret){
      println(row)
    }

  }
}
