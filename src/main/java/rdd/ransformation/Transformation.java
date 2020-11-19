package rdd.ransformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Int;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ligc on 2020/6/21 17:29
 * Transformation算子实战
 */
public class Transformation {
    public static void main(String[] args) {
        myGroupByKey();
    }

    public static void myMap(){
        /**
         * map算子实战：将RDD中的每个元素x2
         * map算子：将RDD中的每个元素，执行自定义函数
         */
        //1.创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("myMap")
                .setMaster("local");
        //2.创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        //3.创建初始RDD，这里使用的是并行化集合
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 67);
        JavaRDD<Integer> nums = sc.parallelize(list);

        //4.使用map算子将初始RDD中每个元素乘以2，组成新的RDD
        JavaRDD<Integer> newNums = nums.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });
        //5.使用foreacha算子，输出乘以2后的算子
        newNums.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println("数组中每个元素，乘以2之后的值为：" + integer);
            }
        });

        //最后，关闭JavaSparkContext
        sc.close();
    }

    public static void myFilter(){
        //1.创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("myFilter")
                .setMaster("local");

        //2.创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        //3.创建集合
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);

        //4.并行化集合创建初始RDD
        JavaRDD<Integer> numbersRDD = sc.parallelize(list);

        //5.使用fillter算子，过滤初始RDD中元素为偶数的元素
        JavaRDD<Integer> oddNumbersRDD = numbersRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer % 2 == 0 ? true : false;
            }
        });

        //使用foreach算子，遍历输出
        oddNumbersRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        //最后，关闭JavaSparkContext
        sc.close();
    }

    /**
     * 将文本行拆分成单词
     */
    public static void myFlatMap(){
        //1. 创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("myFlatMap")
                .setMaster("local");
        //2.创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        //3.从本地文件导入数据，创建初始RDD
        JavaRDD<String> lines = sc.textFile("C:\\Users\\LiJX\\Desktop\\123.txt");

        //4.对初始RDD使用flatMap算子，flatMap算子对于原始RDD中每个元素，也就是一行，可以返回一个或者多个元素
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        //5.输出RDD中的每个元素
        words.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s.toString());
            }
        });
        //最后，关闭JavaSparkContext
        sc.close();
    }

    public static void myGroupByKey(){
        //1.创建SparkConf
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("myGroupByKey");
        //2.创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        //3.创建数据集合
        List<Tuple2<String, Integer>> scores = Arrays.asList(
                new Tuple2<>("class1", 80),
                new Tuple2<>("class2", 90),
                new Tuple2<>("class1", 70),
                new Tuple2<>("class2", 60)
        );

        //4.使用并行化集合，创建初始JavaPairRDD
        //这里，初始化RDD中，每一个元素都是一个Tuple，Tuple的key是String，value是Integer。
        JavaPairRDD<String, Integer> scoresRDD = sc.parallelizePairs(scores);

        //5.对初始化JavaPairRDD使用groupByKey算子
        JavaPairRDD<String, Iterable<Integer>> classScore = scoresRDD.groupByKey();
        //6遍历输出
        classScore.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.println(stringIterableTuple2._1);
                while(stringIterableTuple2._2.iterator().hasNext()){
                    System.out.println(stringIterableTuple2._2.iterator().next());
                }
                System.out.println("=================================================");
            }
        });

        //最后，关闭JavaSparkContext
        sc.close();
    }

}
