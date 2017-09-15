package com.crowley.spark.rdd.sumOfNumbers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class SumOfNumbers {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("numbersSum").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println("enter prime number count till 100 : ");
        Scanner scanner = new Scanner(System.in);
        int till = Integer.valueOf(scanner.nextLine());

        JavaRDD<String> lines = sc.textFile("in/prime_nums.text");
        JavaRDD<String> primes = lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());
        JavaRDD<String> validNumbers = primes.filter(number -> !number.isEmpty());
        List<String> xx = validNumbers.take(till);
        JavaRDD<String> conversion = sc.parallelize(xx);
        JavaRDD<Integer> intPrimes = conversion.map(number -> Integer.valueOf(number));
        Integer primesSum = intPrimes.reduce((x, y) -> x+y);
        System.out.println("sum of first " + till + " Primes = " + primesSum);
    }
}
