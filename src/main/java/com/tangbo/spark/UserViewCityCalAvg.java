package com.tangbo.spark;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Random;

/**
 * 求平均数
 */
public class UserViewCityCalAvg {

    private static Logger logger = Logger.getLogger(UserViewCityCalAvg.class);

    public static final String SEPRATOR = ",";

    public static void main(String[] args) {

        String path = "/temp/" + new Random().nextInt();
        String database = "data_center";
        String table = "data_visit_test";
        SparkConf conf = new SparkConf().setAppName("UserViewCityCalAvg").setMaster("local[4]")
                .set("spark.scheduler.pool", "production");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        try {
            JavaRDD<Row> hiveRDD = sqlContext
                    .sql("select user_id,phone_id from " + database + "." + table).javaRDD();

            JavaRDD<Row> hiveRDD2 = sqlContext
                    .sql("select user_id,phone_id from " + database + "." + table).javaRDD();
            //count
            JavaPairRDD<String, Double> pairRDDCnt = hiveRDD.mapToPair(row -> {
                return new Tuple2<>(String.valueOf(row.getInt(0)), 1.0);
            }).reduceByKey((a, b) -> a + b);

            //sum
            JavaPairRDD<String, Double> pairRDDSum = hiveRDD2.mapToPair(row -> {
                return new Tuple2<String, Double>(String.valueOf(row.getInt(0)),
                        (double) row.getInt(1));
            }).reduceByKey((a, b) -> a + b);
            pairRDDSum.collect().forEach(System.out::println);

            JavaPairRDD<String, Double> pairRDDAvg = pairRDDSum.join(pairRDDCnt)
                    .mapToPair(tuple2 -> {
                        return new Tuple2<String, Double>(tuple2._1, tuple2._2._1 / tuple2._2._2);
                    });

            pairRDDAvg.collect().forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
