package rdd.ransformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;


/**
 * Created by ligc on 2020/6/21 16:05
 * 使用HDFS创建RDD
 * 一样统计字数哈
 */
public class HdfsRDDDemo1 {
    public static void main(String[] args) {
        //1.创建SparkConf，不用加setMaster
        SparkConf conf = new SparkConf()
                .setAppName("HdfsRDDDemo1");
        //2.创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        //3.根据HDFS创建初始化RDD
        JavaRDD<String> lines = sc.textFile("hdfs://192.168.137.129:9000/3568.txt");

        //4.使用map算子统计每一行的字符数量
        JavaRDD<Integer> perLength = lines.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });
        //5.使用reduce算子，统计出总和
        int sum = perLength.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        //打印
        System.out.println("文件中字符数量为：" + sum);
        //最后，关闭JavaSparkContext
        sc.close();
    }
}
