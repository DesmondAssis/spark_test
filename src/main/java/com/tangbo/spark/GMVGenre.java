package com.tangbo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import static com.tangbo.util.TypeConverterUtil.getIntValue;

/**
 * Created by Li.Xiaochuan on 16/10/25.
 */
public class GMVGenre {
    private final static String SPE = ",";

    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: gmv genre <hdfsPath>");
            System.exit(1);
        }
        String path = args[0];
        String hdfsPrefix = args[1];

        SparkConf conf = new SparkConf().setAppName("gmv_genre")
                /*.setMaster("local[4]")*/
                .set("spark.scheduler.pool", "production")
                .set("spark.hadoop.validateOutputSpecs", "false")
                ;
        JavaSparkContext sc = new JavaSparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc);
        // app
        String sql1 = "select tmp.day as day ,tmp.genre as genre ,tmp.channel as channel_type,\n" +
                "       0 as filter_type,\n" +
                "       0 as platform,\n" +
                "       sum(tmp.cost) as num\n" +
                "       from\n" +
                "       (SELECT unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                "        business_order.genre_main as genre,\n" +
                "        business_activity.take_channel as channel,\n" +
                "        total_cost as cost\n" +
                "        FROM data_center.`business_order`\n" +
                "        left join data_center.business_activity on business_order.wzm_activity_id = business_activity.id\n" +
                "        where business_order.STATUS > 1 and business_order.price > 0\n" +
                "        and business_order.is_wap = 0 and business_order.distributor_id =0\n" +
                "       )tmp group by tmp.day, tmp.genre,tmp.channel",
                sql2 = "select  tmp.day as day ,tmp.genre as genre, tmp.channel as channel_type,\n" +
                        "        2 as filter_type,\n" +
                        "        0 as platform,\n" +
                        "        sum(num) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,  business_activity.take_channel as channel,\n" +
                        "        num as num\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        left join data_center.business_activity on business_order.wzm_activity_id = business_activity.id\n" +
                        "        where business_order.STATUS > 1 and business_order.price > 0\n" +
                        "        and business_order.is_wap = 0 and business_order.distributor_id =0\n" +
                        "        )tmp\n" +
                        "        group by tmp.day, tmp.genre, tmp.channel",
                sql3 = "select tmp.day as day ,tmp.genre as genre,tmp.channel as channel_type,\n" +
                        "        1 as filter_type, 0 as platform,\n" +
                        "        count(order_num) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,  business_activity.take_channel as channel,\n" +
                        "        order_num as order_num\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        left join data_center.business_activity on business_order.wzm_activity_id = business_activity.id\n" +
                        "        where business_order.STATUS > 1 and business_order.price > 0\n" +
                        "        and business_order.is_wap = 0 and business_order.distributor_id =0\n" +
                        "        )tmp\n" +
                        "        group by tmp.day,tmp.genre,tmp.channel",
                sql4 = " select tmp.day as day,tmp.genre as genre,(tmp.channel+10) as channel_type,\n" +
                        "        0 as filter_type,\n" +
                        "        0 as platform,\n" +
                        "        sum(total_cost) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,business_activity.sale_channel as channel,\n" +
                        "        total_cost as total_cost\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        left join data_center.business_activity on business_order.wzm_activity_id = business_activity.id\n" +
                        "        where business_order.STATUS > 1 and business_order.price > 0\n" +
                        "        and business_order.is_wap = 0 and business_order.distributor_id =0\n" +
                        "        )tmp\n" +
                        "        group by tmp.day,tmp.genre,tmp.channel",
                sql5 = "select tmp.day as day,tmp.genre as genre, (tmp.channel+10) as channel_type,\n" +
                        "        2 as filter_type,\n" +
                        "        0 as platform,\n" +
                        "        sum(num) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,  business_activity.sale_channel as channel,\n" +
                        "        num as num\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        left join data_center.business_activity on business_order.wzm_activity_id = business_activity.id\n" +
                        "        where business_order.STATUS > 1 and business_order.price > 0\n" +
                        "        and business_order.is_wap = 0 and business_order.distributor_id =0\n" +
                        "        )tmp\n" +
                        "        group by tmp.day,tmp.genre,tmp.channel",
                sql6 = "select tmp.day as day,tmp.genre as genre,(tmp.channel+10) as channel_type,\n" +
                        "        1 as filter_type,\n" +
                        "        0 as platform,\n" +
                        "        count(order_num) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,  business_activity.sale_channel  as channel,\n" +
                        "        order_num as order_num\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        left join data_center.business_activity on business_order.wzm_activity_id = business_activity.id\n" +
                        "        where business_order.STATUS > 1 and business_order.price > 0\n" +
                        "        and business_order.is_wap = 0 and business_order.distributor_id =0\n" +
                        "        )tmp\n" +
                        "        group by tmp.day,tmp.genre,tmp.channel",
                sql7 = "select  tmp.day as day, tmp.genre as genre,20 as channel_type,\n" +
                        "        0 as filter_type,\n" +
                        "        0 as platform,\n" +
                        "        sum(tmp.cost) as num\n" +
                        "        from\n" +
                        "        (SELECT  unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,\n" +
                        "        total_cost as cost\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        where  coupon_name rlike '^.*\\\\u793c\\\\u54c1\\\\u5361.*$'\n" +
                        "        and business_order.STATUS > 1 and business_order.price > 0\n" +
                        "        and business_order.is_wap = 0 and business_order.distributor_id =0\n" +
                        "        )tmp    group by tmp.day, tmp.genre",
                sql8 = "select  tmp.day as day, tmp.genre as genre,20 as channel_type,\n" +
                        "         2 as filter_type,\n" +
                        "         0 as platform,\n" +
                        "        sum(num) as num\n" +
                        "        from\n" +
                        "        (SELECT  unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,\n" +
                        "        num as num\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        where  coupon_name rlike '^.*\\\\u793c\\\\u54c1\\\\u5361.*$'\n" +
                        "        and business_order.STATUS > 1 and business_order.price > 0\n" +
                        "        and business_order.is_wap = 0 and business_order.distributor_id =0\n" +
                        "        )tmp\n" +
                        "        group by tmp.day, tmp.genre",
                sql9 = "select  tmp.day as day, tmp.genre as genre,20 as channel_type,\n" +
                        "        1 as filter_type,\n" +
                        "        0 as platform,\n" +
                        "        count(order_num) as num\n" +
                        "        from\n" +
                        "        (SELECT  unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,20 as channel_type,\n" +
                        "        order_num as order_num\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        where  coupon_name rlike '^.*\\\\u793c\\\\u54c1\\\\u5361.*$'\n" +
                        "        and business_order.STATUS > 1 and business_order.price > 0\n" +
                        "        and business_order.is_wap = 0 and business_order.distributor_id =0\n" +
                        "        )tmp\n" +
                        "        group by tmp.day, tmp.genre",
                sql10 = "select  tmp.day as day, tmp.genre as genre,tmp.channel as channel_type,\n" +
                        "        0 as filter_type,\n" +
                        "        1 as platform,\n" +
                        "        sum(total_cost) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,  business_activity.take_channel as channel,\n" +
                        "        total_cost as total_cost\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        left join data_center.business_activity on business_order.wzm_activity_id = business_activity.id\n" +
                        "        where business_order.STATUS > 1 and business_order.price > 0\n" +
                        "        and business_order.is_wap = 1\n" +
                        "        )tmp\n" +
                        "         group by tmp.day, tmp.genre,tmp.channel",

                sql11 = "select  tmp.day as day, tmp.genre as genre,tmp.channel as channel_type,\n" +
                        "        2 as filter_type,\n" +
                        "        1 as platform,\n" +
                        "        sum(num) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,  business_activity.take_channel as channel,\n" +
                        "        num as num\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        left join data_center.business_activity on business_order.wzm_activity_id = business_activity.id\n" +
                        "        where business_order.STATUS > 1 and business_order.price > 0\n" +
                        "        and business_order.is_wap = 1\n" +
                        "        )tmp\n" +
                        "         group by tmp.day, tmp.genre,tmp.channel",
                sql12 = "select  tmp.day as day, tmp.genre as genre,tmp.channel as channel_type,\n" +
                        "         1 as filter_type,\n" +
                        "         1 as platform,\n" +
                        "        count(order_num) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,  business_activity.take_channel as channel,\n" +
                        "        order_num as order_num\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        left join data_center.business_activity on business_order.wzm_activity_id = business_activity.id\n" +
                        "        where business_order.STATUS > 1 and business_order.price > 0\n" +
                        "        and business_order.is_wap = 1\n" +
                        "        )tmp\n" +
                        "         group by tmp.day, tmp.genre,tmp.channel",
                sql13 = "select  tmp.day as day, tmp.genre as genre,(tmp.channel+10) as channel_type,\n" +
                        "        0 as filter_type,\n" +
                        "        1 as platform,\n" +
                        "        sum(total_cost) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,  business_activity.sale_channel as channel,\n" +
                        "        total_cost as total_cost\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        left join data_center.business_activity on business_order.wzm_activity_id = business_activity.id\n" +
                        "        where business_order.STATUS > 1 and business_order.price > 0\n" +
                        "        and business_order.is_wap = 1\n" +
                        "        )tmp\n" +
                        "        group by tmp.day, tmp.genre,tmp.channel",
                sql14 = "select  tmp.day as day, tmp.genre as genre,(tmp.channel+10) as channel_type,\n" +
                        "         2 as filter_type,\n" +
                        "         1 as platform,\n" +
                        "        sum(num) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,  business_activity.sale_channel as channel,\n" +
                        "        num as num\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        left join data_center.business_activity on business_order.wzm_activity_id = business_activity.id\n" +
                        "        where business_order.STATUS > 1 and business_order.price > 0\n" +
                        "        and business_order.is_wap = 1\n" +
                        "        )tmp\n" +
                        "        group by tmp.day, tmp.genre,tmp.channel",
                sql15 = "select  tmp.day as day, tmp.genre as genre,(tmp.channel+10) as channel_type,\n" +
                        "         1 as filter_type,\n" +
                        "         1 as platform,\n" +
                        "        count(order_num) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,  business_activity.sale_channel as channel,\n" +
                        "        order_num as order_num\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        left join data_center.business_activity on business_order.wzm_activity_id = business_activity.id\n" +
                        "        where business_order.STATUS > 1 and business_order.price > 0\n" +
                        "        and business_order.is_wap = 1\n" +
                        "        )tmp\n" +
                        "        group by tmp.day, tmp.genre,tmp.channel",
                sql16 = "select  tmp.day as day, tmp.genre as genre,20 as channel_type,\n" +
                        "        0 as filter_type,\n" +
                        "        1 as platform,\n" +
                        "        sum(total_cost) as num\n" +
                        "        from\n" +
                        "        (SELECT  unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,\n" +
                        "        total_cost as total_cost\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        where  coupon_name rlike '^.*\\\\u793c\\\\u54c1\\\\u5361.*$'\n" +
                        "        and business_order.STATUS > 1 and business_order.price > 0\n" +
                        "        and business_order.is_wap = 1\n" +
                        "        )tmp\n" +
                        "        group by tmp.day, tmp.genre",
                sql17 = "select  tmp.day as day, tmp.genre as genre,20 as channel_type,\n" +
                        "        2 as filter_type,\n" +
                        "        1 as platform,\n" +
                        "        sum(num) as num\n" +
                        "        from\n" +
                        "        (SELECT  unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,\n" +
                        "        num as num\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        where  coupon_name rlike '^.*\\\\u793c\\\\u54c1\\\\u5361.*$'\n" +
                        "        and business_order.STATUS > 1 and business_order.price > 0\n" +
                        "        and business_order.is_wap = 1\n" +
                        "        )tmp\n" +
                        "        group by tmp.day, tmp.genre",
                sql18 = "select  tmp.day as day, tmp.genre as genre,20 as channel_type,\n" +
                        "         1 as filter_type,\n" +
                        "         1 as platform,\n" +
                        "        count(order_num) as num\n" +
                        "        from\n" +
                        "        (SELECT  unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,\n" +
                        "        order_num as order_num\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        where  coupon_name rlike '^.*\\\\u793c\\\\u54c1\\\\u5361.*$'\n" +
                        "        and business_order.STATUS > 1 and business_order.price > 0\n" +
                        "        and business_order.is_wap = 1\n" +
                        "        )tmp\n" +
                        "        group by tmp.day, tmp.genre",
                sql19 = "select  tmp.day as day, tmp.genre as genre,0 as channel_type,\n" +
                        "        0 as filter_type,\n" +
                        "        2 as platform,\n" +
                        "        sum(total_cost) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        genre_main as genre,\n" +
                        "        total_cost as total_cost\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        where  STATUS > 1 and price > 0\n" +
                        "        and is_wap=0  and distributor_id=2\n" +
                        "        )tmp\n" +
                        "        group by tmp.day, tmp.genre",
                sql20 = "select  tmp.day as day, tmp.genre as genre,0 as channel_type,\n" +
                        "         2 as filter_type,\n" +
                        "         2 as platform,\n" +
                        "        sum(num) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,\n" +
                        "        num as num\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        where STATUS > 1 and price > 0\n" +
                        "        and is_wap=0  and distributor_id=2\n" +
                        "        )tmp\n" +
                        "        group by tmp.day, tmp.genre",

                sql21 = "select  tmp.day as day, tmp.genre as genre,0 as channel_type,\n" +
                        "          1 as filter_type,\n" +
                        "          2 as platform,\n" +
                        "        count(order_num) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,\n" +
                        "        order_num as order_num\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        where  STATUS > 1 and price > 0\n" +
                        "        and is_wap=0  and distributor_id=2\n" +
                        "        )tmp\n" +
                        "        group by tmp.day, tmp.genre",
                sql22 = "select  tmp.day as day, tmp.genre as genre,1 as channel_type,\n" +
                        "        0 as filter_type,\n" +
                        "        2 as platform,\n" +
                        "        sum(total_cost) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        genre_main as genre,\n" +
                        "        total_cost as total_cost\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        where  STATUS > 1 and price > 0\n" +
                        "        and is_wap=0  and distributor_id=1\n" +
                        "        )tmp\n" +
                        "        group by tmp.day, tmp.genre",
                sql23 = "select  tmp.day as day, tmp.genre as genre,1 as channel_type,\n" +
                        "        2 as filter_type,\n" +
                        "        2 as platform,\n" +
                        "        sum(num) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,  1 as channel_type,\n" +
                        "        num as num\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        where STATUS > 1 and price > 0\n" +
                        "        and is_wap=0  and distributor_id=1\n" +
                        "        )tmp\n" +
                        "        group by tmp.day, tmp.genre",
                sql24 = "select  tmp.day as day, tmp.genre as genre,1 as channel_type,\n" +
                        "        1 as filter_type,\n" +
                        "        2 as platform,\n" +
                        "        count(order_num) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,\n" +
                        "        order_num as order_num\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        where  STATUS > 1 and price > 0\n" +
                        "        and is_wap=0  and distributor_id=1\n" +
                        "        )tmp\n" +
                        "        group by tmp.day, tmp.genre",
                sql25 = "select  tmp.day as day, tmp.genre as genre,2 as channel_type,\n" +
                        "         0 as filter_type,\n" +
                        "         2 as platform,\n" +
                        "        sum(total_cost) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        genre_main as genre,\n" +
                        "        total_cost as total_cost\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        where  STATUS > 1 and price > 0\n" +
                        "        and is_wap=0  and distributor_id >2\n" +
                        "        )tmp\n" +
                        "        group by tmp.day, tmp.genre",
                sql26 = "select  tmp.day as day, tmp.genre as genre,2 as channel_type,\n" +
                        "         2 as filter_type,\n" +
                        "         2 as platform,\n" +
                        "        sum(num) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,\n" +
                        "        num as num\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        where  STATUS > 1 and price > 0\n" +
                        "        and is_wap=0  and distributor_id >2\n" +
                        "        )tmp\n" +
                        "        group by tmp.day, tmp.genre",
                sql27 = "select  tmp.day as day, tmp.genre as genre,2 as channel_type,\n" +
                        "        1 as filter_type,\n" +
                        "        2 as platform,\n" +
                        "        count(order_num) as num\n" +
                        "        from\n" +
                        "        (SELECT unix_timestamp(from_unixtime(business_order.create_time,'yyyyMMdd'),'yyyyMMdd') as day,\n" +
                        "        business_order.genre_main as genre,\n" +
                        "        order_num as order_num\n" +
                        "        FROM data_center.`business_order`\n" +
                        "        where  STATUS > 1 and price > 0\n" +
                        "        and is_wap=0  and distributor_id > 2\n" +
                        "        )tmp\n" +
                        "        group by tmp.day, tmp.genre";


        JavaRDD<Row> hiveRDD1 = getJavaRDD(sqlContext, sql1);
        JavaRDD<Row> hiveRDD2 = getJavaRDD(sqlContext, sql2);
        JavaRDD<Row> hiveRDD3 = getJavaRDD(sqlContext, sql3);
        JavaRDD<Row> hiveRDD4 = getJavaRDD(sqlContext, sql4);
        JavaRDD<Row> hiveRDD5 = getJavaRDD(sqlContext, sql5);
        JavaRDD<Row> hiveRDD6 = getJavaRDD(sqlContext, sql6);
        JavaRDD<Row> hiveRDD7 = getJavaRDD(sqlContext, sql7);
        JavaRDD<Row> hiveRDD8 = getJavaRDD(sqlContext, sql8);
        JavaRDD<Row> hiveRDD9 = getJavaRDD(sqlContext, sql9);
        JavaRDD<Row> hiveRDD10 = getJavaRDD(sqlContext, sql10);

        JavaRDD<Row> hiveRDD11 = getJavaRDD(sqlContext, sql11);
        JavaRDD<Row> hiveRDD12 = getJavaRDD(sqlContext, sql12);
        JavaRDD<Row> hiveRDD13 = getJavaRDD(sqlContext, sql13);
        JavaRDD<Row> hiveRDD14 = getJavaRDD(sqlContext, sql14);
        JavaRDD<Row> hiveRDD15 = getJavaRDD(sqlContext, sql15);
        JavaRDD<Row> hiveRDD16 = getJavaRDD(sqlContext, sql16);
        JavaRDD<Row> hiveRDD17 = getJavaRDD(sqlContext, sql17);
        JavaRDD<Row> hiveRDD18 = getJavaRDD(sqlContext, sql18);
        JavaRDD<Row> hiveRDD19 = getJavaRDD(sqlContext, sql19);
        JavaRDD<Row> hiveRDD20 = getJavaRDD(sqlContext, sql20);

        JavaRDD<Row> hiveRDD21 = getJavaRDD(sqlContext, sql21);
        JavaRDD<Row> hiveRDD22 = getJavaRDD(sqlContext, sql22);
        JavaRDD<Row> hiveRDD23 = getJavaRDD(sqlContext, sql23);
        JavaRDD<Row> hiveRDD24 = getJavaRDD(sqlContext, sql24);
        JavaRDD<Row> hiveRDD25 = getJavaRDD(sqlContext, sql25);
        JavaRDD<Row> hiveRDD26 = getJavaRDD(sqlContext, sql26);
        JavaRDD<Row> hiveRDD27 = getJavaRDD(sqlContext, sql27);

        hiveRDD1.union(hiveRDD2)
                .union(hiveRDD3)
                .union(hiveRDD4)
                .union(hiveRDD5)
                .union(hiveRDD6)
                .union(hiveRDD7)
                .union(hiveRDD8)
                .union(hiveRDD9)
                .union(hiveRDD10)
                .union(hiveRDD11)
                .union(hiveRDD12)
                .union(hiveRDD13)
                .union(hiveRDD14)
                .union(hiveRDD15)
                .union(hiveRDD16)
                .union(hiveRDD17)
                .union(hiveRDD18)
                .union(hiveRDD19)
                .union(hiveRDD20)
                .union(hiveRDD21)
                .union(hiveRDD22)
                .union(hiveRDD23)
                .union(hiveRDD24)
                .union(hiveRDD25)
                .union(hiveRDD26)
                .union(hiveRDD27).map(GMVGenre::getRowContent1)

                .saveAsTextFile(hdfsPrefix + path);

    }


    public static JavaRDD<Row> getJavaRDD(HiveContext sqlContext, String sql) {
        return  sqlContext.sql(sql).javaRDD();
    }

    @SuppressWarnings("Duplicates")
    public static String getRowContent1(Row row) {
        int day = getIntValue(row.get(0));
        int genre = getIntValue(row.get(1));
        int channelType = getIntValue(row.get(2));
        int filterType = getIntValue(row.get(3));
        int platform = getIntValue(row.get(4));
        int num = getIntValue(row.get(5));

        return day + SPE + genre + SPE + channelType + SPE + filterType + SPE + platform + SPE + num;
    }

    /*


export  --connect jdbc:mysql://test5.wanzhoumo.com:3307/data_center?useUnicode=true&characterEncoding=utf8 --table ss_channel_city_day --export-dir /tmp/channel/city -m 4 --username wanzhoumo --password wanzhoumo123 --batch --columns "day,city,channel_type,filter_type,platform,num" --input-fields-terminated-by "," --fields-terminated-by "," --update-mode allowinsert --update-key "day,city,channel_type,filter_type,platform"

export  --connect jdbc:mysql://test5.wanzhoumo.com:3307/data_center?useUnicode=true&characterEncoding=utf8 --table ss_channel_gerne_day --export-dir /tmp/channel/genre -m 4 --username wanzhoumo --password wanzhoumo123 --batch --columns "day,gerne,channel_type,filter_type,platform,num" --input-fields-terminated-by "," --fields-terminated-by "," --update-mode allowinsert --update-key "day,gerne,channel_type,filter_type,platform"


     */
}

