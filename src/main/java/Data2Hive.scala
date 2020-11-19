import org.apache.spark.sql.SparkSession

/**
 * Created by ligc on 2020/11/18 22:18
 * 编写在Hive数据库traffic_db中建表并导入数据的应用程序
 */
object Data2Hive {
  def main(args: Array[String]): Unit = {
    //万变不离其宗，构建SparkSession
    val spark:SparkSession = SparkSession.builder()
      .appName("Data2Hive")
      .enableHiveSupport() //开启Hive支持
      .getOrCreate()

    //使用数据库traffic_db
    spark.sql("use traffic_db2")
    //在hive中创建表 monitor_flow_action
    spark.sql("create table if not exists monitor_flow_action " +
      "(data STRING, monitor_id STRING, camera_id STRING, car STRING," +
      "action_time STRING, speed STRING, road_id STRING, area_id STRING) " +
      " row format delimited fields terminated by '\t'")
    //导入数据到表monitor_flow_action
    spark.sql("load data local inpath" +
      " '/soft/monitor_flow_action'" +
      " into table monitor_flow_action")

    //创建表monitor_camera_info
    spark.sql("drop table if exists monitor_camera_info")
    spark.sql("create table if not exists monitor_camera_info" +
      " (monitor_id STRING, camera_id STRING)" +
      " row format delimited fields terminated by '\t'")

    //导入数据
    spark.sql("load data local inpath" +
      " '/soft/monitor_camera_info' " +
      " into table monitor_camera_info")
    println("============data2hive finish =====================")
  }
}
