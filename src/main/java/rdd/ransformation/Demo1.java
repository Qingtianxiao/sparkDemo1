package rdd.ransformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by ligc on 2020/6/5 20:56
 */
public class Demo1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("map")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numnbers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numbersRdd = sc.parallelize(numnbers);

    }
}
