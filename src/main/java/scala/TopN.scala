package scala
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession


/**
 * Created by ligc on 2020/11/17 7:45
 */
object TopN {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark:SparkSession = SparkSession.builder()
      .master("local")
      .appName("demo4")
      .getOrCreate()

    val d1 = spark.read.textFile("C:\\Users\\LiJX\\Desktop\\data\\2015-flight.txt")
    d1.show()
    import spark.implicits._
//    val personDataset = d1.map(line =>{
//      if(StringUtils.isNotEmpty(line)){
//        val fields = line.split(",")
//        val dest = fields(0)
//        val origin = fields(1)
//        val count = fields(2).toInt
//        MyRow(dest, origin, count)
//      }
//    })
//    personDataset.show()
  }

}
