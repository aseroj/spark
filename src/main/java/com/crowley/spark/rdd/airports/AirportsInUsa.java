package com.crowley.spark.rdd.airports;

import com.crowley.spark.rdd.commons.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class AirportsInUsa {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("airportsInUsa").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> airports = sc.textFile("in/airports.text");
        JavaRDD<String> airportsInUsa =
                airports.filter(line -> line.split(Utils.COMMA_DELIMITER)[3].equals("\"United States\""));
        JavaRDD<String> airportsNameAndCityNames =
                airportsInUsa.map(line -> {
                            String[] splits = line.split(Utils.COMMA_DELIMITER);
                            return StringUtils.join(new String[]{splits[1], splits[2]}, ",");
                        }
                );
        airportsNameAndCityNames.saveAsTextFile("out/airports_in_usa.text");
    }
}
