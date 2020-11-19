package rdd.ransformation;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 使用json文件创建dataFrame
 * Created by ligc on 2020/9/8 16:57
 */
public class DataFrameCreate {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("DataFrameCreate")
                .master("local")
                .config("spark.sql.warehouse.dir", "C:\\Users\\LiJX\\Desktop\\blog\\warehouse")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("C:\\Users\\LiJX\\Desktop\\dataframe.txt");
        df.show();

    }
}
