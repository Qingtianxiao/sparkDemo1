package rdd.ransformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.List;

/**
 * Created by ligc on 2020/8/27 21:03
 * 文本内取top3
 */
public class Top3 {
    public static void main(String[] args) {
        //1.创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("Top3")
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        //创建初始RDD
        JavaRDD<String> linesRDD = jsc.textFile("C:\\Users\\LiJX\\Desktop\\top3_2.txt");

       //转换成JavapairRDD
        JavaPairRDD<Integer, String> numbersPairRDD = linesRDD.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<>(Integer.valueOf(s), s);
            }
        });

        //排序
        JavaPairRDD<Integer, String> sortedPairRDD = numbersPairRDD.sortByKey(false);

        //取key或者value就可以了哈
        JavaRDD<Integer> sortedRDD = sortedPairRDD.map(new Function<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, String> t) throws Exception {
                return t._1;
            }
        });

        List<Integer> list = sortedRDD.take(3);

        for(Integer i : list){
            System.out.println(i);
        }



        jsc.close();
    }
}
