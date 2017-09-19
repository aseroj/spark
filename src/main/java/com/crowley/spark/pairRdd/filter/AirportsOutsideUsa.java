package com.crowley.spark.pairRdd.filter;

import com.crowley.spark.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class AirportsOutsideUsa {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("airportsOutsideUsa").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> airports = sc.textFile("in/airports.text");
        JavaPairRDD<String, String> airportsRdd = airports.mapToPair(getAirportNameAndCountryPair());
        JavaPairRDD<String, String> airportsOutsideUsa = airportsRdd.filter(keyValue -> !keyValue._2().equals("\"United States\""));
        airportsOutsideUsa.saveAsTextFile("out/airports_outside_usa.text");
    }

    private static PairFunction<String, String, String> getAirportNameAndCountryPair() {
        return (PairFunction<String, String, String>) line -> new Tuple2<>(line.split(Utils.COMMA_DELIMITER)[1], line.split(Utils.COMMA_DELIMITER)[3]);
    }
}
