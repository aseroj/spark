package com.crowley.spark.pairRdd.sort;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Map;

public class WordCount {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("wordCountSorted").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("in/word_count.text");
        JavaRDD<String> wordRdd = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordPairRdd = wordRdd.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> wordCounts = wordPairRdd.reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> x + y);

        // for flipping the key and value
        JavaPairRDD<Integer, String> countWordPairs = wordCounts.mapToPair(wordToCount -> new Tuple2<>(wordToCount._2(), wordToCount._1()));
        JavaPairRDD<Integer, String> sortedHousePriceAvg = countWordPairs.sortByKey(false);

        for(Tuple2<Integer, String> wordCountPair : sortedHousePriceAvg.collect()) {
            System.out.println(wordCountPair._2() + " : " + wordCountPair._1());
        }
    }

}
