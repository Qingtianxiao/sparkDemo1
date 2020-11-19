package scala


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row

/**
 * Created by ligc on 2020/11/18 13:23
 */
object SparkSqlJDBC {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSqlJDBC")
      .master("local")
      .getOrCreate()

    //创建存生信息的RDD
    val studentsRDD:RDD[String] = spark.sparkContext.parallelize(Array("4 xiaoming 26", "5 xiaogang 27"))
    val studentsTableRDD :RDD[Array[String]] = studentsRDD.map(_.split(" "))
    //通过StructType指定每个字段的schema
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )

    //将studentsTableRDD转换为rowRDD
    val rowRDD = studentsTableRDD.map(line =>
      Row(line(0).toInt, line(1).trim, line(2).toInt)
    )

    //建立rowRDD和schema之间的对应关系，返回DataFram
    val studentDF = spark.createDataFrame(rowRDD, schema)

    val mp = Map(
      ("driver", "com.mysql.jdbc.Driver"),
      ("url","jdbc:mysql://192.168.137.131:3306/mysql"),
      ("dbtable","student3"),
      ("user","root"),
      ("password", "lll030921")
    )
    studentDF.write.mode("append")
      .format("jdbc")
      .options(mp)
      .save()
  }

}
