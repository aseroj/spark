package com.crowley.spark.rdd.airports;

import com.crowley.spark.rdd.commons.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsByLat {
    public static void main(String[] args) {
//        lats larger than 40 and output the airport name and lat pair
//        lat is 6th col
//        lat pair 6, 7

        SparkConf conf = new SparkConf().setAppName("airportsByLat").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> airports = sc.textFile("in/airports.text");
        JavaRDD<String> airportsInUsa = airports.filter(line -> Float.valueOf(line.split(Utils.COMMA_DELIMITER)[6]) > 70);

        JavaRDD<String> airportsNameAndLatLng =
                airportsInUsa.map(line -> {
                            String[] splits = line.split(Utils.COMMA_DELIMITER);
                            return StringUtils.join(new String[]{splits[1], splits[6], splits[7]}, ",");
                        }
                );
        airportsNameAndLatLng.saveAsTextFile("out/airports_by_lat.text");
    }

}

