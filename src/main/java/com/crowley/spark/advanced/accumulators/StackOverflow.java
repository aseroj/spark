package com.crowley.spark.advanced.accumulators;

import com.crowley.spark.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Option;



public class StackOverflow {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("StackOverflow").setMaster("local[1]");
        SparkContext sparkContext = new SparkContext(conf);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

        final LongAccumulator total = new LongAccumulator();
        final LongAccumulator missingSalaryMidPoint = new LongAccumulator();
        final LongAccumulator processedBytes = new LongAccumulator();

        total.register(sparkContext, Option.apply("total"), false);
        missingSalaryMidPoint.register(sparkContext, Option.apply("missing salary middle points"), false);
        processedBytes.register(sparkContext, Option.apply("processed bytes"), true);

        JavaRDD<String> responseRdd = javaSparkContext.textFile("in/2016-stack-overflow-survey-responses.csv");

        JavaRDD<String> responseFromCanada = responseRdd.filter( response -> {
            processedBytes.add(response.getBytes().length);
            String[] splits = response.split(Utils.COMMA_DELIMITER, -1);
            total.add(1);
            if (splits[14].isEmpty()) {
                missingSalaryMidPoint.add(1);
            }
            return splits[2].equals("Canada");
        });

        System.out.println("responses count from canada : " + responseFromCanada.count());
        System.out.println("total responses count : " + total.value());
        System.out.println("missing salary responses count : " + missingSalaryMidPoint.value());
        System.out.println("bytes processed : " + processedBytes.value());
    }

}
