package com.tangbo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by Li.Xiaochuan on 16/10/25.
 */
public class DataDayVisitUv {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("data_day_visit.uv").setMaster("local[4]").set("spark.scheduler.pool", "production");
        JavaSparkContext sc = new JavaSparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc);
//        String sql = "select unix_timestamp(part_key,'yyyy-MM-dd') as day, \"whole\" as type, visit_page, \"\" as page_id, count(distinct phone_id) as user_view from data_center.data_visit_year  group by part_key,visit_page ";
        String sql = "select 1 as a, 2 as b from data_center.data_visit limit 1";
        DataFrame frame = sqlContext.sql(sql);
        frame.show();
        JavaRDD<Row> hiveRDD = sqlContext.sql(sql).javaRDD();

//        hiveRDD.map(row -> {
//            int day = row.getInt(0);
//            String type = row.getString(1);
//            String visitPage = row.getString(2);
//            String pageId = row.getString(3);
//            long userView = row.getLong(4);
//
//            return day + "," + type + "," + visitPage + "," + pageId + "," + userView;
//        }).saveAsTextFile("hdfs://master:9000/tmp/testdata2/");




        //
        sql = "select 1 as a, 3 as b from data_center.data_visit limit 1";
        JavaRDD<Row> hiveRDD1 = sqlContext.sql(sql).javaRDD();

        hiveRDD.map(row -> {
            int a = row.getInt(0);
            int b = row.getInt(1);

            return a + "," + b;
        }).union(hiveRDD1.map(row -> {
            int a = row.getInt(0);
            int b = row.getInt(1);

            return a + "," + b;
        }))

                .saveAsTextFile("hdfs://master:9000/tmp/testdata7/");

    }
}

