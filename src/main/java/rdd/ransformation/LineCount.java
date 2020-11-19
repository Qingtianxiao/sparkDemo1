package rdd.ransformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Int;
import scala.Tuple2;

/**
 * Created by ligc on 2020/6/21 16:29
 * 对出现向同行进行统计
 */
public class LineCount {
    public static void main(String[] args) {
        //1.创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("LineCount")
                .setMaster("local");

        //2.创建JavaSparkConf
        JavaSparkContext sc = new JavaSparkContext(conf);

        //3.创建初始RDD，使用本地文件
        JavaRDD<String> lines = sc.textFile("C:\\Users\\LiJX\\Desktop\\lineCount.txt");
        //4.对初始RDD使用mapToPair算子
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        //5.对pairs算子使用reduceByKey算子
        JavaPairRDD<String, Integer> count = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        //6.使用action算子foreach
        count.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + " appears " + stringIntegerTuple2._2 + " times.");
            }
        });
        //最后，关闭JavaSparkContext
        sc.close();
    }
}
