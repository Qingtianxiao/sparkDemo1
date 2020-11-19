package rdd.ransformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;

/**
 * Created by ligc on 2020/8/26 22:50
 * 累加变量
 */
public class MyAccumulator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Accumulator");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        //累加变量
//        AccumulatorV2<Integer> sum = jsc.

        jsc.close();
    }
}
