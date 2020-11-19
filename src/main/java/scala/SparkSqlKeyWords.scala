package scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
 * Created by ligc on 2020/11/18 20:36
 * 每天热点关键词统计
 */
object SparkSqlKeyWords {
  def main(args: Array[String]): Unit = {
    //1.万变不离其宗，创建SparkSession
    val spark:SparkSession = SparkSession.builder()
      .master("local")
      .appName("sparkSqlKeyWords")
      .getOrCreate()

    //2.读入文件
    val fileRDD: RDD[String] = spark.sparkContext.textFile("C:\\Users\\LiJX\\Desktop\\data\\keywords.txt")

    //3.转换成RDD[Tuple],((日期，关键词)，用户)
    val tupleRDD : RDD[((String, String), String)] = fileRDD.map(
//      line => ((line.split(",")(0), line.split(",")(1)), line.split(",")(2))
        line => {
          val date = line.split(",")(0)
          val user = line.split(",")(1)
          val keyword = line.split(",")(2)
          ((date, keyword), user)
        }
    )
    //根据（日期，关键词），进行分组，获取每天每个关键词被哪些用户进行了搜索,groupbyKey,返回的还是一个元组，（key，Iterable）
    val groupRDD:RDD[((String, String), Iterable[String])] = tupleRDD.groupByKey()

    //对每天每个用户搜索的关键词进行去重，并统计去重后的数量，获得其uv
    val uvRDD:RDD[((String, String), Int)] = groupRDD.map(
      line => {
        val dateAndKeyword:(String, String) = line._1
        //用户数据去重
        val users:Iterator[String] = line._2.iterator
        val distinctUsers = new ListBuffer[String]()
        while(users.hasNext){
          val user = users.next()
          if(!distinctUsers.contains(user)){
            distinctUsers += user
          }
        }
        val uv = distinctUsers.size
        (dateAndKeyword, uv)
      }
    )
    //构建schema
    val schema:StructType = StructType(
      Array(
        StructField("date", StringType, true),
        StructField("keyword", StringType,true),
        StructField("uv", IntegerType, true)
      )
    )
    //将uvRDD改造成RDD[ROW]
    val rowRDD:RDD[Row] = uvRDD.map(
      line => {
        val date = line._1._1
        val keyWords = line._1._2
        val uv = line._2
        Row(date, keyWords, uv)
      }
    )
    //构造DataFrame
    val df = spark.createDataFrame(rowRDD, schema).sort("date")
    df.show()

    df.createTempView("date_keyword_uv")
//    spark.sql("" +
//      "select date,keyword,uv" +
//      "from (" +
//      "select date, keyword, uv, row_number() over (partition by date order by uv desc) rank" +
//      "from date_keyword_uv)t" +
//      "where t.rank<=3").show()
  }
}
