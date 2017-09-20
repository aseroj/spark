package com.crowley.spark.advanced.broadcast;

import com.crowley.spark.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.broadcast.Broadcast;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class UkMakerSpaces {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("UkMakerSpaces").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        final Broadcast<Map<String, String>> postcodeMap = sc.broadcast(loadPostcodeMap());
        JavaRDD<String> makerSpaceRdd = sc.textFile("in/uk-makerspaces-identifiable-data.csv");

        JavaRDD<String> regions = makerSpaceRdd
                .filter(line -> !line.split(Utils.COMMA_DELIMITER, -1)[0].equals("Timestamp"))
                .map(line -> {
                    Optional<String> postPrefix = getPostPrefix(line);
                    if(postPrefix.isPresent() && postcodeMap.value().containsKey(postPrefix.get())) {
                        return postcodeMap.value().get(postPrefix.get());
                    }
                    return "Unknown";
                });
        for(Map.Entry<String, Long> regionCounts : regions.countByValue().entrySet()) {
            System.out.println(regionCounts.getKey() + " : " + regionCounts.getValue());
        }
    }

    private static Optional<String> getPostPrefix(String line) {
        String[] splits = line.split(Utils.COMMA_DELIMITER, -1);
        String postcode = splits[4];
        if(postcode.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(postcode.split(" ")[0]);
    }

    private static Map<String, String> loadPostcodeMap() throws FileNotFoundException {
        Scanner postCode = new Scanner(new File("in/uk-postco de.csv"));
        Map<String, String> postcodeMap = new HashMap<>();
        while (postCode.hasNextLine()) {
            String line = postCode.nextLine();
            String[] splits = line.split(Utils.COMMA_DELIMITER, -1);
            postcodeMap.put(splits[0], splits[7]);
        }
        return postcodeMap;
    }

}
