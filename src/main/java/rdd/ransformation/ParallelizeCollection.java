package rdd.ransformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by ligc on 2020/7/29 21:54
 * 并行化集合创建初始RDD
 */
public class ParallelizeCollection {
    SparkConf conf = new SparkConf()
            .setAppName("parallelizeCollection")
            .setMaster("local");

    JavaSparkContext sc = new JavaSparkContext(conf);

    List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8,9, 10);

    JavaRDD<Integer> parallelizeNumbers = sc.parallelize(numbers);







}
