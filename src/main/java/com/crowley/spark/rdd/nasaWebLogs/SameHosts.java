package com.crowley.spark.rdd.nasaWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SameHosts {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sameHosts").setMaster("local[1]");
        JavaSparkContext sc  = new JavaSparkContext(conf);

        JavaRDD<String> julyLogs = sc.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> augLogs = sc.textFile("in/nasa_19950801.tsv");
        JavaRDD<String> julyHosts = julyLogs.map(line -> line.split("\t")[0]);
        JavaRDD<String> augHosts = augLogs.map(line -> line.split("\t")[0]);

        JavaRDD<String> intersection = julyHosts.intersection(augHosts);

        JavaRDD<String> cleanHostIntersections = intersection.filter(line -> isNotHeader(line));
        cleanHostIntersections.saveAsTextFile("out/nasa_logs_same_hosts.csv");
    }

    private static boolean isNotHeader(String line) {
        return !(line.startsWith("host") && line.contains("bytes"));
    }

}
