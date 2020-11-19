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
import java.util.List;


/**
 * Created by ligc on 2020/8/25 16:22
 */
public class TransformationOperation {
    public static void main(String[] args) {
        //1.测试map算子
//        map();

        //2.测试filter算子
//        filter();

        //3.测试flatMap算子
//        flatMap();

        //4.测试groupByKey算子
//        groupByKey();

        //5.测试reduceByKey算子
//        reduceByKey();

        //6.测试sortByKey算子
//        sortByKey();

        //7.测试join算子
//        join();

        //8.测试cogroup算子
//        cogroup();


    }



    /**
     * map算子案例：将集合中所有的元素乘以2
     */
    private static void map(){
        //1.创建sparkConf
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("map");

        //2.创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        //3.创建并行化集合
        JavaRDD<Integer> listRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        //4.使用map算子，针对listRDD中的每个元素，都乘以2
        JavaRDD<Integer> newRDD = listRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return 2 * integer;
            }
        });

        //5.使用foreach算子，遍历输出新的RDD中的每个元素
        newRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        //最后，关闭SparkContext
        sc.close();
    }

    /**
     * filter算子：过滤集合中的偶数
     */
    private static void filter(){
        //1.创建SparkConf
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("filter");

        //2.创建JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //3.创建并行化集合
        JavaRDD<Integer> listRDD = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        //4.使用filter算子
        JavaRDD<Integer> newRDD = listRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer % 2 == 0;
            }
        });

        //5.遍历输出新的RDD
        newRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        //最后，关闭JavaSparkContext
        jsc.close();
    }

    /**
     * 3.flatMap：将行拆分成单词
     */
    private static void flatMap(){
        //1.创建sparkConf
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("flatMap");

        //2.创建javaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //3.创建集合
        List<String> list = Arrays.asList("hello you", "hello me", "hello world");

        //4.创建初始RDD
        JavaRDD<String> linesRDD = jsc.parallelize(list);

        //5.使用flatMap算子，flatMap是针对输入RDD的每一个元素，进行一系列用户自定义的操作之后，返回的一个或多个元素
        //这(些)元素封装成了一个集合，即Iterator对象
        //flatMap算子接收的参数是FlatMapFunction对象
        //但是新的RDD中，参数竟然是String而不是Iterator，Amazing
        JavaRDD<String> words = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        //6.使用foreach算子，遍历输出
        words.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        //最后，关闭JavaSparkContext
        jsc.close();
    }

    /**
     * 4.groupByKey：将每个班级的成绩进行分组
     */
    private static void groupByKey(){
        //1.创建SparkConf
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("groupByKey");

        //2.创建JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //3.创建模拟集合
        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("class1", 90),
                new Tuple2<String, Integer>("class2", 89),
                new Tuple2<String, Integer>("class1", 88),
                new Tuple2<String,Integer>("class2", 61)
        );

        //4.创建初始RDD
        JavaPairRDD<String, Integer> scoreRDD = jsc.parallelizePairs(list);

        //5.使用groupByKey算子
        //这里返回的是一个JavaPairRDD，两个参数，第一个是String，就是Key的类型，第二个是Value封装成的集合，也就是
        //Iterable<value>类型
        JavaPairRDD<String, Iterable<Integer>> newRDD = scoreRDD.groupByKey();

        //6.遍历输出
        newRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t2) throws Exception {
                System.out.print(t2._1 + ": ");
                Iterator<Integer> iterator = t2._2.iterator();
                while(iterator.hasNext()){
                    System.out.print(iterator.next() + "\t");
                }
                System.out.println();
            }
        });

        //最后，关闭JavaSparkContext
        jsc.close();
    }

    /**
     * 5.reduceByKey：统计每个班级的总分
     */
    private static void reduceByKey(){
        //1.创建SparkConf
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("reduceByKey");

        //2.创建JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //3.创建集合
        List<Tuple2<String, Integer>> score = Arrays.asList(
                new Tuple2<>("class1", 60),
                new Tuple2<>("class2", 70),
                new Tuple2<>("class1", 60),
                new Tuple2<>("class2", 70)
        );

        //4.创建并行化集合
        JavaPairRDD<String, Integer> scoresRDD = jsc.parallelizePairs(score);

        //5.使用groupByKey算子，统计每个班级的总分
        JavaPairRDD<String, Integer> newRDD = scoresRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        //6.遍历输出
        newRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + ": " + stringIntegerTuple2._2 + ".");
            }
        });

        //最后，关闭JavaSparkContext
        jsc.close();
    }

    /**
     * 6.sortByKey：将学生分数进行排序
     */
    private static void sortByKey(){
        //1.创建SparkConf
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("sortByKey");

        //2.创建JavaSparkContext
        JavaSparkContext jsc =new JavaSparkContext(conf);

        //3.创建集合
        List<Tuple2<String, Integer>> scores = Arrays.asList(
                new Tuple2<>("ligc", 200),
                new Tuple2<>("lijx", 159),
                new Tuple2<>("ligy", 180)
        );

        //4.创建初始RDD
        JavaPairRDD<String, Integer> scoresRDD = jsc.parallelizePairs(scores);

        //5.使用mapToPair算子，颠倒key-value
        JavaPairRDD<Integer, String> scoresRESRDD = scoresRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        });

        //6.使用sortByKey算子进行排序
        JavaPairRDD<Integer,String> sortedRESRDD = scoresRESRDD.sortByKey(false);

        //7.使用mapToPair算子，颠倒key-value
        JavaPairRDD<String, Integer> sortedRDD = sortedRESRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return new Tuple2<>(integerStringTuple2._2, integerStringTuple2._1);
            }
        });

        //8.遍历输出
        sortedRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + ": " + stringIntegerTuple2._2);
            }
        });
    }

    /**
     * 7.join：打印每个学生的成绩
     */
    private static void join(){
        //1.创建SparkConf
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("join");

        //2.创建JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //3.模拟集合
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<>(1, "ligc"),
                new Tuple2<>(2, "lijx"),
                new Tuple2<>(3, "ligy"),
                new Tuple2<>(4, "ligc"),
                new Tuple2<>(1, "tom")
        );

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<>(1, 100),
                new Tuple2<>(2, 50),
                new Tuple2<>(3, 10),
                new Tuple2<>(1, 200)
        );

        //4.创建初始RDD，两个并行化集合
        JavaPairRDD<Integer, String> studentRDD = jsc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scoreRDD = jsc.parallelizePairs(scoreList);

        //5.使用join算子
        JavaPairRDD<Integer, Tuple2<String, Integer>> studentScoreRDD = studentRDD.join(scoreRDD);

        //6.遍历输出
        studentScoreRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> integerTuple2Tuple2) throws Exception {
                System.out.println(integerTuple2Tuple2._2._1 +
                        ", id=" + integerTuple2Tuple2._1 +
                        ", score=" +integerTuple2Tuple2._2._2);
            }
        });

        //最后，关闭JavaSparkContext
        jsc.close();
    }

    /**
     * 8.cogroup：打印每个学生的成绩
     */
    private static void cogroup(){
        //1.创建SparkConf
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("cogroup");

        //2.创建JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //3.创建并行化集合
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<>(1, "ligc"),
                new Tuple2<>(2, "lijx"),
                new Tuple2<>(3, "ligy"),
                new Tuple2<>(4, "ligc"),
                new Tuple2<>(1, "tom")
        );

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<>(1, 100),
                new Tuple2<>(2, 50),
                new Tuple2<>(3, 10),
                new Tuple2<>(1, 200)
        );

        //4.创建初始RDD，两个并行化集合
        JavaPairRDD<Integer, String> studentRDD = jsc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scoreRDD = jsc.parallelizePairs(scoreList);

        //5.使用cogroup算子
        JavaPairRDD<Integer,Tuple2<Iterable<String>,Iterable<Integer>>> studentScoreRDD
                =  studentRDD.cogroup(scoreRDD);

        //6.使用foreach算子遍历输出
        studentScoreRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
                System.out.println(t._1);
                System.out.println(t._2._1);
                System.out.println(t._2._2);
                System.out.println("================================");
            }
        });
    }
}
