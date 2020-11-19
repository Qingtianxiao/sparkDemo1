package scala

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Created by ligc on 2020/11/18 12:39
 */
object LineCount {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local")
      .appName("LineCount")
      .getOrCreate()

    //你说，我明明就有mysql，为什么不用呢
    val mp = Map(
      ("driver", "com.mysql.jdbc.Driver"),
      ("url","jdbc:mysql://192.168.137.131:3306/mysql"),
      ("dbtable","employee_tbl"),
        ("user","root"),
      ("password", "lll030921")
    )

    //加载数据
    val jdbcDF = spark.read.format("jdbc").options(mp).load()
    //显示数据
    jdbcDF.show()


  }

}
