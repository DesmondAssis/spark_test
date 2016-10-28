package com.tangbo.spark;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Calendar;
import java.util.Date;
import java.util.Random;

/**
 * 按照key求交集
 */
public class UserViewCity {

    private static Logger logger = Logger.getLogger(UserViewCity.class);

    public static final String SEPRATOR = ",";

    public static void main(String[] args) {

        String path = "/temp/" + new Random().nextInt();
        String database = "data_center";
        String table = "data_visit_year";
        SparkConf conf = new SparkConf().setAppName("uv_statis").setMaster("local[4]")
                .set("spark.scheduler.pool", "production");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        try {
            JavaRDD<Row> hiveRDD = sqlContext
                    .sql("select user_id,city,channel from " + database + "." + "data_temp")
                    .javaRDD();

            JavaPairRDD<String, Integer> pairRDD = hiveRDD.mapToPair(row -> {
                return new Tuple2<>(
                        row.getInt(0) + SEPRATOR + row.getInt(1) + SEPRATOR + row.getString(2)
                        + SEPRATOR + "&", 1);
            });

            JavaRDD<Row> hiveRDD2 = sqlContext
                    .sql("select user_id,city,channel from " + database + "." + table
                         + " where part_key in ('2016-05-16')").javaRDD();

            JavaPairRDD<String, Integer> pairRDD2 = hiveRDD2.mapToPair(row -> {
                return new Tuple2<>(
                        row.getInt(0) + SEPRATOR + row.getInt(1) + SEPRATOR + row.getString(2)
                        + SEPRATOR + "&", 1);
            });
            //join
            JavaPairRDD<String, Integer> result = pairRDD2.join(pairRDD).mapToPair(row -> {
                String key = row._1();
                return new Tuple2<>(key, 1);
            }).reduceByKey((a, b) -> a + b);

            result.collect().forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
