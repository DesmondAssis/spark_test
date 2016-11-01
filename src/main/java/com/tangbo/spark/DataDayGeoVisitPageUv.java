package com.tangbo.spark;

import com.tangbo.util.DateTimeUtil;
import com.tangbo.util.StringUtil;
import com.tangbo.util.TypeConverterUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by tangbo on 16/9/19.
 */
public class DataDayGeoVisitPageUv {

	public static final String SEPRATOR = ",";

	public static void main(String[] args) {

		if (args.length < 2) {
			System.err.println("Usage: pageUv <hdfsPath>");
			System.exit(1);
		}
		String path = args[0];
		String hdfsPrefix = args[1];

		SparkConf conf = new SparkConf().setAppName("data_day_visit.page.uv")
				/*.setMaster("local[4]")*/
				.set("spark.scheduler.pool", "production")
				.set("spark.hadoop.validateOutputSpecs", "false")
				;
		JavaSparkContext sc = new JavaSparkContext(conf);

		HiveContext sqlContext = new HiveContext(sc);
		try {
			JavaRDD<Row> hiveRDD = sqlContext.sql("select phone_id,visit_time,visit_page,page_id,province,city from data_center.data_visit_year where part_key >='2016-01-01' ").javaRDD();

			JavaPairRDD<String, Integer> pairRDD = hiveRDD.
					filter(row -> {
						Long dayZero = DateTimeUtil.getUvDayZero(TypeConverterUtil.getLongValue(row.get(1)));
						String visitPage = row.getString(2);
						return dayZero > 0 && !("0".equals(visitPage) || visitPage == null || "null".equals(visitPage));
					})
					.mapToPair(row -> {
						Integer phoneId = TypeConverterUtil.getIntValue(row.get(0));
						Long dayZero = DateTimeUtil.getUvDayZero(TypeConverterUtil.getLongValue(row.get(1)));
						String visitPage = StringUtil.trimToEmpty(row.getString(2)).toLowerCase();
						String pageId = StringUtil.trimToEmpty(row.getString(3)).toLowerCase();
						Integer provice = TypeConverterUtil.getIntValue(row.get(4));
						Integer city = TypeConverterUtil.getIntValue(row.get(5));

						int cityId = city != null && city > 0 ? city : provice > 0 ? provice : 0;

						return new Tuple2<>(dayZero + SEPRATOR + phoneId + SEPRATOR + visitPage + SEPRATOR + pageId + SEPRATOR + cityId , 1);
					}).reduceByKey((a, b) -> a + b);

			JavaPairRDD<String, Integer> result = pairRDD.mapToPair(tuple -> {
				String[] temp = tuple._1().split(SEPRATOR);
				return new Tuple2<>(temp[0] + SEPRATOR + "page" + SEPRATOR + temp[2] + SEPRATOR + temp[3] + SEPRATOR + temp[4] , 1);
			}).reduceByKey((a, b) -> a + b);

			result.map(tuple2 -> tuple2._1() + SEPRATOR + tuple2._2())
//					.collect().forEach(System.out::println)
					.saveAsTextFile(hdfsPrefix + path)
			;
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
