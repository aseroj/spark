package com.crowley.spark.pairRdd.join;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;
import java.util.Arrays;

public class JoinOperations {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("JoinOperations").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> ages = sc.parallelizePairs(Arrays.asList(new Tuple2<>("Tom", 29), new Tuple2<>("John", 22)));
        JavaPairRDD<String, String> addresses= sc.parallelizePairs(Arrays.asList(new Tuple2<>("James", "USA"), new Tuple2<>("John", "UK")));

        // best practice for keeping the operations on the same partition
        // ---------------------------------------------------------------
        HashPartitioner partitioner = new HashPartitioner(20);
        ages.partitionBy(partitioner);
        addresses.partitionBy(partitioner);
        // ---------------------------------------------------------------

        JavaPairRDD< String, Tuple2<Integer, String>> join = ages.join(addresses);
        join.saveAsTextFile("out/age_address_join.text");

        JavaPairRDD<String, Tuple2<Integer, Optional<String>>> leftOuterjoin = ages.leftOuterJoin(addresses);
        leftOuterjoin.saveAsTextFile("out/age_address_left_outer_join.text");

        JavaPairRDD<String, Tuple2<Optional<Integer>, String>> rightOuterjoin = ages.rightOuterJoin(addresses);
        rightOuterjoin.saveAsTextFile("out/age_address_right_outer_join.text");

        JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<String>>> fullOuterjoin = ages.fullOuterJoin(addresses);
        fullOuterjoin.saveAsTextFile("out/age_address_full_outer_join.text");

    }
}
