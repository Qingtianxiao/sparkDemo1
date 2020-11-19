package rdd.ransformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Int;

import java.util.Arrays;
import java.util.List;

/**
 * Created by ligc on 2020/8/26 22:34
 * 广播变量
 */
public class BroadcastVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("broadcastVariable");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);

        final int factor  = 3;
        final Broadcast<Integer> broadcastVariable = jsc.broadcast(factor);

        JavaRDD<Integer> numberRDD = jsc.parallelize(list);

        JavaRDD<Integer> multiRDD= numberRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                int factor = broadcastVariable.value();
                return factor * integer;
            }
        });

        multiRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        jsc.close();
    }

}
