package scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Created by ligc on 2020/11/18 19:57
 * 统计用户UV（每天的用户访问量）
 */
object SparkSqlAggFunctionDemo {
  def main(args: Array[String]): Unit = {
    //1.创建sparkSession
    val spark:SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkSqlAggFuctionDemo")
      .getOrCreate()
    //导入函数
    import org.apache.spark.sql.functions._
    //2.构造测试数据
    val arr:Array[String] = Array(
      "2019-06-01,0001",
      "2019-06-01,0001",
      "2019-06-01,0002",
      "2019-06-01,0003",
      "2009-06-02,0001",
      "2009-06-02,0003"
    )

    //构建初始RDD
    val arrRDD:RDD[String] = spark.sparkContext.parallelize(arr)

    //创建schema
    val schema:StructType = StructType(
        Array(
          StructField("date", StringType, true),
          StructField("userid", IntegerType, true)
        )
    )
    val rowRDD : RDD[Row] = arrRDD.map(line => Row(line.split(",")(0), line.split(",")(1).toInt))

    //创建DataFrame
    val arrDF:DataFrame = spark.createDataFrame(rowRDD, schema)

    //最后一步，使用DataFrame的弱类型操作
    val resultDF : DataFrame = arrDF.groupBy("date")
      .agg(countDistinct("userid") as "count")

    resultDF.show()

    //3.构建初始RDD，这里的Row是通过tuple创建的，amazing
//    val rowRDD :RDD[Row] = spark.sparkContext.parallelize(arr)
//      .map(line => Row(line.split(",")(0), line.split(",")(1).toInt)
//      )
//
//    //4.构建schema
//    val structType:StructType = StructType(
//      Array(
//        StructField("date",StringType, true),
//        StructField("userid", IntegerType, true)
//      )
//    )
//
//    //将RDD[Row]转换成DataFrame
//    val df:DataFrame = spark.createDataFrame(rowRDD, structType)
//    //final，聚合查询，根据日期分组，然后将每一组的用户ID去重后统计数量
//    df.groupBy("date")
//      .agg(countDistinct("userid") as "count")
//      .show()

  }
}
