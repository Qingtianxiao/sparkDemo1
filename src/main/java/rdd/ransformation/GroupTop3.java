package rdd.ransformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * Created by ligc on 2020/8/27 19:43
 * 分组取top3
 */
public class GroupTop3 {
    public static void main(String[] args) {
        //1.创建sparkConf
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("GroupTop3");

        //2.创建JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //3.创建初始RDD
        JavaRDD<String> linesRDD = jsc.textFile("C:\\Users\\LiJX\\Desktop\\top3.txt");

        //4.转换成key-value形式
        JavaPairRDD<String, Integer> classScoreRDD = linesRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[0], Integer.valueOf(s.split(" ")[1]));
            }
        });

        //5.分组
        JavaPairRDD<String, Iterable<Integer>> groupClassScoreRDD = classScoreRDD.groupByKey();

        //6.取top3，自己取
        JavaPairRDD<String, Iterable<Integer>> top3ClassScoreRDD = groupClassScoreRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                PriorityQueue<Integer> queue = new PriorityQueue<>(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o2 - o1;
                    }
                });
                Integer[] top3 = new Integer[3];
                Iterator<Integer> iterator = t._2.iterator();
                while(iterator.hasNext()){
                    queue.add(iterator.next());
                }
                for(int i = 0; i < 3; i ++){
                    top3[i] = queue.poll();
                }
                return new Tuple2<>(t._1, Arrays.asList(top3));
            };
        });

        //遍历输出
        top3ClassScoreRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println(t._1);
                Iterator<Integer> iterator = t._2.iterator();
                while(iterator.hasNext()){
                    System.out.println(iterator.next());
                }
                System.out.println("===============================");
            }
        });

        //最后，关闭JavaSparkContext
        jsc.close();

    }
}
