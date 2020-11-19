package rdd.ransformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Int;

/**
 * Created by ligc on 2020/6/21 15:56
 * 根据本地文件创建初始RDD
 * 统计文本中字符数量
 */
public class LocalRDDDemo2 {
    public static void main(String[] args) {
        //1.创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("LocalRDDDemo2")
                .setMaster("local");

        //2.创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        //3.创建初始化RDD，使用SparkContext以及其子类的testFile()方法
        JavaRDD<String> lines = sc.textFile("C:\\Users\\LiJX\\Desktop\\35688.txt");
        //4.使用map算子，获得每一行的长度
        JavaRDD<Integer> length = lines.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });

        //5.使用reduce算子，对全文的length求和
        int sum = length.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        System.out.println(sum);

        //6.关闭javaSparkContext
        sc.close();
    }
}
