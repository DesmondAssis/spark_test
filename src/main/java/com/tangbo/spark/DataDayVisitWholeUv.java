package com.tangbo.spark;

import com.tangbo.util.DateTimeUtil;
import com.tangbo.util.StringUtil;
import com.tangbo.util.TypeConverterUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by Li.Xiaochuan on 16/10/25.
 */
public class DataDayVisitWholeUv {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: gmv city <hdfsPath>");
            System.exit(1);
        }
        String path = args[0];
        String hdfsPrefix = args[1];

        SparkConf conf = new SparkConf().setAppName("data_day_visit.uv")
                /*.setMaster("local[4]")*/
                .set("spark.scheduler.pool", "production")
                .set("spark.hadoop.validateOutputSpecs", "false")
                ;
        JavaSparkContext sc = new JavaSparkContext(conf);

        HiveContext sqlContext = new HiveContext(sc);
        String sql = "select unix_timestamp(part_key,'yyyy-MM-dd') as day, \"whole\" as type, visit_page, \"\" as page_id, count(distinct phone_id) as user_view from data_center.data_visit_year where part_key >='2016-01-01' group by part_key,visit_page ";

        JavaRDD<Row> hiveRDD = sqlContext.sql(sql).javaRDD();

        hiveRDD
                .filter(row -> {
                    int day = TypeConverterUtil.getIntValue(row.get(0));
                    String visitPage = StringUtil.trimToEmpty(row.getString(2)).toLowerCase();
                    return day > 0 && !("0".equals(visitPage) || visitPage == null || "null".equals(visitPage));
                })
                .map(row -> {
            int day = TypeConverterUtil.getIntValue(row.get(0));
            String type = row.getString(1);
            String visitPage = StringUtil.trimToEmpty(row.getString(2)).toLowerCase();
            String pageId = row.getString(3);
            int userView = TypeConverterUtil.getIntValue(row.get(4));

            return day + "," + type + "," + visitPage + "," + pageId + "," + userView;
        }).saveAsTextFile(hdfsPrefix + path);

    }

    /* sqoop export

export  --connect jdbc:mysql://test5.wanzhoumo.com:3307/data_center?useUnicode=true&characterEncoding=utf8 --table data_day_visit_uv --export-dir /tmp/spark/${wf:id()} -m 4 --username wanzhoumo --password wanzhoumo123 --batch --columns "day,type,visit_page,page_id,user_view" --input-fields-terminated-by "," --fields-terminated-by "," --update-mode allowinsert --update-key "day,type,visit_page,page_id"

     */
}

