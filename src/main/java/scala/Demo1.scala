package scala

import org.apache.spark.sql.SparkSession

/**
 * Created by ligc on 2020/11/16 18:46
 * 这个真的是scala版本的第一个demo，希望可以成功
 */
class Demo1 {
}
object  Demo1{
  def main(args: Array[String]): Unit = {
    //创建sparkSession，这个是sql下面的class
    val spark:SparkSession = SparkSession.builder()
      .appName("demo1")
      .master("local")
      .getOrCreate();

    //读数据
    val flightData2015 = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("C:\\Users\\LiJX\\Desktop\\data\\2015-flight.txt")

//    flightData2015.show()
    spark.conf.set("spark.sql.shuffle.partitions",5)

//    println(flightData2015.sort("count").take(2))
    //将任何DataFrame注册为数据表或视图，并使用纯SQL对它进行查询
    flightData2015.createOrReplaceTempView("flight_data_2015")

    val sqlWayDF = spark.sql("select dest_country_name, count(1) from flight_data_2015 group by dest_country_name")

    //找到前五个国家
    val top5 = spark.sql("select dest_country_name,count(1) as total from flight_data_2015 " +
      "group by dest_country_name order by count(1) desc limit 5")
    top5.show()

  }
}
