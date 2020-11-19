package rdd.ransformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Int;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by ligc on 2020/8/27 11:54
 */
public class SortedWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("sortedWordCount");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        //1.构建初始化RDD
        JavaRDD<String> linesRDD = jsc.textFile("C:\\Users\\LiJX\\Desktop\\35688.txt");

        //2.分词
        JavaRDD<String> wordRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        //3.原元素为key，构建k-v，v=1
        JavaPairRDD<String, Integer> wordKVRDD = wordRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        //4.计算sum(v)
        JavaPairRDD<String, Integer> wordSumKVRDD = wordKVRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer t1, Integer t2) throws Exception {
                return t1 + t2;
            }
        });

        //转换key<->value
        JavaPairRDD<Integer, String> wordVKRDD =
                wordSumKVRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                        return new Tuple2<>(t._2, t._1);
                    }
                });

        //使用sortByKey
        JavaPairRDD<Integer, String> wordSortedVKRDD = wordVKRDD.sortByKey(false);

        //k<->v
        JavaPairRDD<String,Integer> wordSortedKVRDD = wordSortedVKRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<>(t._2, t._1);
            }
        });

        //遍历输出
        wordSortedKVRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + " appears " + t._2 + " times");
            }
        });

        jsc.close();
    }
}
