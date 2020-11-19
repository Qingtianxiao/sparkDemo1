package rdd.ransformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Int;
import scala.Tuple2;

/**
 * Created by ligc on 2020/8/27 13:01
 * 1.实现自定义的key，要实现Ordered接口和Serializable接口，在key中实现自己对多个列的排序算法
 * 2. 将包含文本的RDD，映射成key为自定义key，value为文本的javaPairRDD
 * 3. 使用sortByKey进行排序
 * 4.剔除key
 */
public class SecondarySort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("SecondarySort");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        //创建初始RDD
        JavaRDD<String> linesRDD= jsc.textFile("C:\\Users\\LiJX\\Desktop\\num.txt");

        //构建key-value形式，其中key使用的是自定义的key，为了reduceByKey算子做铺垫
        JavaPairRDD<SecondarySortedKey, String> myKeyLinesRDD = linesRDD.mapToPair(new PairFunction<String, SecondarySortedKey, String>() {
            @Override
            public Tuple2<SecondarySortedKey, String> call(String s) throws Exception {
                String[] lineArray = s.split(" ");
                int first = Integer.valueOf(lineArray[0]);
                int second = Integer.valueOf(lineArray[1]);
                SecondarySortedKey myKey = new SecondarySortedKey(first, second);
                return new Tuple2<>(myKey, s);
            }
        });

        //使用sortByKey算子
        JavaPairRDD<SecondarySortedKey,String> sortedMyKeyLinesRDD = myKeyLinesRDD.sortByKey();

        //剔除myKey
        JavaRDD<String> resultRDD = sortedMyKeyLinesRDD.map(new Function<Tuple2<SecondarySortedKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondarySortedKey, String> t) throws Exception {
                return t._2;
            }
        });

        resultRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });


    }
}
