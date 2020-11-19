package rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * Created by ligc on 2020/6/5 15:06
 */
public class RddDemo1{
    public static void main(String[] args) {
        //创建sparkconf
        SparkConf conf = new SparkConf()
                .setAppName("RddDemo1");
        conf.set("spark.testing.memory", 512 * 1024 * 1024 + "");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("hdfs://192.168.137.129:9000/35688.txt");
        JavaRDD<Integer> lineLengthes = lines.map(new Function<String, Integer>() {
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });

        int count = lineLengthes.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        System.out.println("文件总字数数是：" + count);
    }

}
