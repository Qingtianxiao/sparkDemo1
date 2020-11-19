package scala

import org.apache.spark.sql.SparkSession

/**
 * Created by ligc on 2020/11/17 12:32
 */
class Demo2 {

}
object Demo2{
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local")
      .appName("demo2")
      .getOrCreate()

    //导入spark的隐式转换
    import spark.implicits._
    //导入spark sql的function
    import org.apache.spark.sql.functions._


    //创建初始化DF
    val employeeDF = spark.read.json("D:\\ligc\\百度云下载\\spark从入门到精通\\第256讲-Spark 2.0-Dataset开发详解-初步体验untypd操作案例：计算部门平均年龄与薪资\\文档\\exployee.json")

    val departmentDF = spark.read.json("D:\\ligc\\百度云下载\\spark从入门到精通\\第256讲-Spark 2.0-Dataset开发详解-初步体验untypd操作案例：计算部门平均年龄与薪资\\文档\\department.json")

    //进行计算操作
    //先对employee进行过滤，只统计20岁以上的员工
    employeeDF.filter("age > 20")
      .join(departmentDF, $"depId" === $"id")
    //需要跟department数据进行join，然后才能根据部门名称和员工性别进行聚合
      .groupBy(departmentDF("name"), employeeDF("gender"))
      .agg(avg(employeeDF("salary")), avg(employeeDF("age")))
      .show()
  }
}
