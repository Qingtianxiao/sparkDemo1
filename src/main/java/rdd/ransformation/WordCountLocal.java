package rdd.ransformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Iterator;


/**
 * Created by ligc on 2020/6/20 21:08
 * 本地测试的wordCount程序
 */
public class WordCountLocal {
    public static void main(String[] args) {
        //编写spark应用程序
        //本地执行，是可以直接在ide中的main()方法中执行的
        //第一步：创建SparkConf对象，设置Spark应用的配置信息
        //使用setMaster()可以设置Spark Application要连接的Spark集群的master节点的url

        //如果是将spark application在集群中运行，需要需改两个地方
        //一、将setMaster()方法删除
        //二、将文件换为HDFS文件
        //本地运行环境
        SparkConf conf = new SparkConf()
                .setAppName("WordCountLocal")
                .setMaster("local");
        //集群运行环境
//        SparkConf conf = new SparkConf()
//                .setAppName("WordCountLocal");
        //第二步，创建JavaSparkContext对象
        //在Spark中，SparkContext是Spark所有功能的一个入口，你无论是用java、scala，甚至是python编写都必须有一个SparkContext，它
        //的主要作用，包括初始化Spark Application所需的一些核心组件，包括调度器（DAGSchedule、TaskSchedule），还会到Spark Master
        //节点进行注册..
        //above all，sparkContext对象是Spark Application中最重要的一个对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        //第三步：针对输入源（本地文件、HDFS、Hive），创建初始的RDD
        //输入源中的数据会被打散，分配到RDD的每个partition中，从而形成初始的分布式数据集
        //在SparkContext中，用于根据文件类型的输入源创建RDD的方法叫做textFile()方法
        //在Java中，创建的普通RDD，都叫做JavaRDD
        //本地运行环境
        JavaRDD<String> lines = sc.textFile("C:\\Users\\LiJX\\Desktop\\35688.txt");
        //集群运行环境
//        JavaRDD<String> lines  =sc.textFile("hdfs://192.168.137.129:9000/35688.txt");

        //第四步：对初始RDD进行transformation操作，也就是一些计算操作
        //先将每一行拆分成单个的单词
        //通常操作会通过创建function，并配合RDD的ma、flatMap等算子来执行function
        //通常，如果比较简单，则创建指定Function的匿名内部类
        //但如果function比较复杂，则创建单独的一个类，作为实现这个function接口的类
        //FlatMapFunction，有两个泛型参数，分别代表了输入和输出
        //我们这里呢，肯定是String，因为代表了一行一行的文本；输出，其实也是String，因为是每一行的文本
        //flatMap算子的作用，其实呢，就是将RDD的一个元素，拆分成一个或多个元素
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        //接着，需要将每一个单词映射成（单词，1）的这种格式
        //因为只有这样，后面才能根据单词作为key，对每个单词出现的次数进行累加
        //mapToPair，其实就是将每个元素映射成<k1,v1>、<k2，v2>这种Tuple2类型的元素
        //这里的Tuple2类型就是scala类型
        //mapToPair这个算子，要求的是与PairFunction配合使用，第一个参数代表输入类型
        //第二个和第三参数相当于输出的Tuple2的k和v的类型
        //JavaPairRDD两个泛型参数，分别代表了Tuple2第一个值和第二个值的类型
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        //接着，需要以单词作为key，统计每个单词出现的次数
        //这里要使用reduceByKey这个算子，对每个key对应的value都进行reduce操作
        //reduce操作，相当于把第一个值和第二个值进行计算，然后将计算的结果跟第三个值进行计算，以此类推
        JavaPairRDD<String,Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        //到这里为止，我们通过几个spark算子操作，已经统计出了单词的次数
        //但是，我们之前使用的flatMap、mapToPair、reduceByKey这种操作，叫做transformation操作
        //一个spark应用中，光有transformation操作是不行的，必须要有action操作
        //最后，可以使用一种叫做action的操作，比如说foreach操作，来触发程序的执行
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1 + " appeared " + wordCount._2 + " times.");
            }
        });

        //最后关闭JavaSparkContext
        sc.close();
    }
}
