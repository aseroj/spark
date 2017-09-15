package com.crowley.spark.rdd.nasaWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UnionLog {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("unionLogs").setMaster("local[1]");
        JavaSparkContext sc  = new JavaSparkContext(conf);

        JavaRDD<String> julyLogs = sc.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> augLogs = sc.textFile("in/nasa_19950801.tsv");

        JavaRDD<String> aggregatedLogLines = julyLogs.union(augLogs);
        JavaRDD<String> cleanLogLines = aggregatedLogLines.filter(line -> isNotHeader(line));
        JavaRDD<String> sample = cleanLogLines.sample(true, 0.1);
        sample.saveAsTextFile("out/sample_nasa_logs.csv");
    }

    private static boolean isNotHeader(String line) {
        return !(line.startsWith("host") && line.contains("bytes"));
    }

}
