package com.crowley.spark.pairRdd.groupByKey;

import com.crowley.spark.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;

public class AirportsByCountry {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("airportsByCountry").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("in/airports.text");
        JavaPairRDD<String, String> countryAndAirportNamePair =
                lines.mapToPair(airport -> new Tuple2<>(airport.split(Utils.COMMA_DELIMITER)[3], airport.split(Utils.COMMA_DELIMITER)[1]));
        JavaPairRDD<String, Iterable<String>> airportsByCountry = countryAndAirportNamePair.groupByKey();

        for (Map.Entry<String, Iterable<String>> airports : airportsByCountry.collectAsMap().entrySet()) {
            System.out.println(airports.getKey() + " : " + airports.getValue());
        }
    }
}
