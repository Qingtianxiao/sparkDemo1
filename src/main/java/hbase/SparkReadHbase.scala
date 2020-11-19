package hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.mapreduce
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

/**
 * Created by ligc on 2020/11/19 20:18
 */
object SparkReadHbase {
  def main(args: Array[String]): Unit = {
    //万变不离其宗
    val spark:SparkSession = SparkSession.builder()
      .appName("SparkReadHbase")
      .master("local")
      .getOrCreate()

    val sparkContext:SparkContext = spark.sparkContext

    //设置HBase配置信息
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "192.168.137.129")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "student")

    //读取hbase的表并转换成RDD
    val hbaseInputRDD:RDD[(ImmutableBytesWritable, Result)] = sparkContext.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    //输出RDD中的数据
    hbaseInputRDD.foreach(println(_))
  }
}
